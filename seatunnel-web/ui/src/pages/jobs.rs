// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job list with submit dialog and cancel action.

use crate::api;
use crate::app::{poll_interval, RefreshControl};
use crate::fmt::{fmt_duration, fmt_time};
use crate::ui::{ErrorBanner, Modal, StateTag, SuccessBanner};
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_navigate;

// Streaming demo: unbounded FakeSource (row.num = -1) with a 10ms
// inter-row delay, so throughput/idle metrics and live logs are visible.
const DEFAULT_CONFIG: &str = r#"env:
  job.name: streaming-demo
  parallelism: 1
  checkpoint:
    interval: 10000

source:
  FakeSource:
    row.num: -1
    sleep.ms: 10

sink:
  Console: {}
"#;

#[component]
pub fn Jobs() -> impl IntoView {
    let (jobs, set_jobs) = RwSignal::new_local(Vec::<api::JobSummary>::new()).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let refresh = expect_context::<RefreshControl>();

    spawn_local(async move {
        loop {
            if refresh.0.get_untracked() {
                match api::jobs().await {
                    Ok(value) => {
                        set_jobs.set(value);
                        set_error.set(None);
                    }
                    Err(err) => set_error.set(Some(err)),
                }
            }
            gloo_timers::future::TimeoutFuture::new(poll_interval()).await;
        }
    });

    let show_submit = RwSignal::new(false);
    let navigate = use_navigate();

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        <div class="toolbar">
            <button class="primary" on:click=move |_| show_submit.set(true)>"Submit job"</button>
            <span class="muted">{move || jobs.get().len().to_string()}" jobs"</span>
        </div>
        <div class="panel">
            <table>
                <thead>
                    <tr>
                        <th>"Job"</th>
                        <th>"Job ID"</th>
                        <th>"State"</th>
                        <th>"Started"</th>
                        <th>"Duration"</th>
                        <th>"Actions"</th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        jobs.get()
                            .into_iter()
                            .map(|job| {
                                let job_id = job.job_id.clone();
                                let cancel_id = job.job_id.clone();
                                let link_id = job.job_id.clone();
                                let navigate = navigate.clone();
                                view! {
                                    <tr>
                                        <td>{job.job_name.clone()}</td>
                                        <td class="mono">
                                            <a
                                                href=format!("/jobs/{}", link_id)
                                                on:click=move |event| {
                                                    event.prevent_default();
                                                    navigate(
                                                        &format!("/jobs/{}", job_id),
                                                        Default::default(),
                                                    );
                                                }
                                            >{job.job_id.clone()}</a>
                                        </td>
                                        <td><StateTag state=job.state.clone() /></td>
                                        <td>{fmt_time(job.start_time_ms)}</td>
                                        <td>{fmt_duration(job.start_time_ms, job.end_time_ms)}</td>
                                        <td>
                                            <CancelJobButton job_id=cancel_id />
                                        </td>
                                    </tr>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tbody>
            </table>
        </div>
        <SubmitJobDialog show=show_submit />
    }
}

/// Cancel button with a confirm dialog; refreshes nothing itself (the
/// polling loop picks up the state change).
#[component]
fn CancelJobButton(job_id: String) -> impl IntoView {
    let busy = RwSignal::new(false);
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let id = job_id.clone();

    view! {
        <button
            class="danger"
            disabled=move || busy.get()
            on:click=move |_| {
                let id = id.clone();
                if window().confirm_with_message(&format!("Cancel job {}?", id)).unwrap_or(false) {
                    busy.set(true);
                    spawn_local(async move {
                        if let Err(err) = api::cancel_job(&id).await {
                            set_error.set(Some(err));
                        }
                        busy.set(false);
                    });
                }
            }
        >"Cancel"</button>
        {move || error.get().map(|err| view! { <span class="muted">{err}</span> })}
    }
}

/// Job submission dialog: config text + format + name + parallelism.
#[component]
fn SubmitJobDialog(show: RwSignal<bool>) -> impl IntoView {
    let config_text = RwSignal::new(DEFAULT_CONFIG.to_string());
    let format = RwSignal::new("yaml".to_string());
    let job_name = RwSignal::new(String::new());
    let parallelism = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let (success, set_success) = RwSignal::new_local(None::<String>).split();

    let on_submit = move |_| {
        if busy.get_untracked() {
            return;
        }
        let parallelism = parallelism
            .get_untracked()
            .trim()
            .parse::<i32>()
            .ok()
            .filter(|value| *value > 0);
        let request = api::SubmitJobRequest {
            config_text: config_text.get_untracked(),
            format: format.get_untracked(),
            job_name: {
                let name = job_name.get_untracked();
                (!name.trim().is_empty()).then_some(name)
            },
            parallelism,
        };
        busy.set(true);
        set_error.set(None);
        spawn_local(async move {
            match api::submit_job(request).await {
                Ok(result) => {
                    set_success.set(Some(format!("Job {} submitted: {}", result.job_id, result.message)));
                    show.set(false);
                }
                Err(err) => set_error.set(Some(err)),
            }
            busy.set(false);
        });
    };

    view! {
        <Modal show title="Submit job">
            <SuccessBanner message=Signal::derive(move || success.get()) />
            <ErrorBanner message=Signal::derive(move || error.get()) />
            <div class="field">
                <label>"Job config"</label>
                <textarea
                    prop:value=move || config_text.get()
                    on:input=move |event| config_text.set(event_target_value(&event))
                ></textarea>
            </div>
            <div class="form-row">
                <div class="field">
                    <label>"Format"</label>
                    <select on:change=move |event| format.set(event_target_value(&event))>
                        <option value="yaml" selected=true>"yaml"</option>
                        <option value="toml">"toml"</option>
                        <option value="hocon">"hocon"</option>
                    </select>
                </div>
                <div class="field">
                    <label>"Job name (optional)"</label>
                    <input
                        type="text"
                        prop:value=move || job_name.get()
                        on:input=move |event| job_name.set(event_target_value(&event))
                    />
                </div>
                <div class="field">
                    <label>"Parallelism (optional)"</label>
                    <input
                        type="number"
                        min="1"
                        prop:value=move || parallelism.get()
                        on:input=move |event| parallelism.set(event_target_value(&event))
                    />
                </div>
            </div>
            <div class="modal-footer">
                <button on:click=move |_| show.set(false)>"Close"</button>
                <button class="primary" disabled=move || busy.get() on:click=on_submit>
                    {move || if busy.get() { "Submitting…".to_string() } else { "Submit".to_string() }}
                </button>
            </div>
        </Modal>
    }
}
