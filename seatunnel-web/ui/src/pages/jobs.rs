// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job list with submit dialog and stop action.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::{fmt_duration, fmt_time};
use crate::i18n::{lang, t, tf};
use crate::ui::{push_toast, ConfirmDialog, ErrorBanner, Modal, StateTag, ToastKind};
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

    use_polling(move || {
        spawn_local(async move {
            match api::jobs().await {
                Ok(value) => {
                    set_jobs.set(value);
                    set_error.set(None);
                    mark_refreshed();
                }
                Err(err) => set_error.set(Some(err)),
            }
        })
    });

    let show_submit = RwSignal::new(false);
    let navigate = use_navigate();

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        <div class="toolbar">
            <button class="primary" on:click=move |_| show_submit.set(true)>
                {move || t("jobs.submit")}
            </button>
            <span class="muted">{move || tf("jobs.count", &[&jobs.get().len().to_string()])}</span>
        </div>
        <div class="panel">
            <table>
                <thead>
                    <tr>
                        <th>{move || t("jobs.col.job")}</th>
                        <th>{move || t("jobs.col.job_id")}</th>
                        <th>{move || t("jobs.col.state")}</th>
                        <th>{move || t("jobs.col.started")}</th>
                        <th>{move || t("jobs.col.duration")}</th>
                        <th>{move || t("jobs.col.actions")}</th>
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

/// Stop button with a confirm dialog (stop = final checkpoint, savepoint
/// semantics); errors surface as toasts.
#[component]
fn CancelJobButton(job_id: String) -> impl IntoView {
    let busy = RwSignal::new(false);
    let confirm_open = RwSignal::new(false);
    let message = Signal::derive({
        let job_id = job_id.clone();
        move || tf("jobs.cancel_confirm", &[job_id.as_str()])
    });
    let stop = {
        let job_id = job_id.clone();
        Callback::new(move |_| {
            busy.set(true);
            let job_id = job_id.clone();
            spawn_local(async move {
                if let Err(err) = api::cancel_job(&job_id).await {
                    push_toast(ToastKind::Error, format!("{}: {}", t("jobs.cancel_failed"), err));
                }
                busy.set(false);
            });
        })
    };

    view! {
        <button
            class="danger"
            disabled=move || busy.get()
            on:click=move |_| confirm_open.set(true)
        >{move || t("jobs.cancel")}</button>
        {move || {
            // Re-created here so the labels follow a language switch; the
            // open/busy state lives in signals and survives re-creation.
            let _ = lang().get();
            let title = tf("jobs.cancel_title", &[job_id.as_str()]);
            view! {
                <ConfirmDialog
                    show=confirm_open
                    title=title
                    message=message
                    confirm_label=t("jobs.cancel")
                    danger=true
                    on_confirm=stop.clone()
                />
            }
        }}
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

    let on_submit = move |_| {
        if busy.get_untracked() {
            return;
        }
        // Surface an invalid parallelism instead of silently dropping it.
        let raw_parallelism = parallelism.get_untracked().trim().to_string();
        let parallelism = if raw_parallelism.is_empty() {
            None
        } else {
            match raw_parallelism.parse::<i32>() {
                Ok(value) if value > 0 => Some(value),
                _ => {
                    set_error.set(Some(t("jobs.parallelism_invalid")));
                    return;
                }
            }
        };
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
                    push_toast(ToastKind::Success, tf("jobs.submitted", &[&result.job_id]));
                    show.set(false);
                }
                Err(err) => {
                    set_error.set(Some(format!("{}: {}", t("jobs.submit_failed"), err)));
                }
            }
            busy.set(false);
        });
    };

    view! {
        <Modal show title=Signal::derive(move || t("jobs.dialog_title"))>
            <ErrorBanner message=Signal::derive(move || error.get()) />
            <div class="field">
                <label>{move || t("jobs.config")}</label>
                <textarea
                    prop:value=move || config_text.get()
                    on:input=move |event| config_text.set(event_target_value(&event))
                ></textarea>
            </div>
            <div class="form-row">
                <div class="field">
                    <label>{move || t("jobs.format")}</label>
                    <select on:change=move |event| format.set(event_target_value(&event))>
                        <option value="yaml" selected=true>"yaml"</option>
                        <option value="toml">"toml"</option>
                        <option value="hocon">"hocon"</option>
                    </select>
                </div>
                <div class="field">
                    <label>{move || t("jobs.name_opt")}</label>
                    <input
                        type="text"
                        prop:value=move || job_name.get()
                        on:input=move |event| job_name.set(event_target_value(&event))
                    />
                </div>
                <div class="field">
                    <label>{move || t("jobs.parallelism_opt")}</label>
                    <input
                        type="number"
                        min="1"
                        prop:value=move || parallelism.get()
                        on:input=move |event| parallelism.set(event_target_value(&event))
                    />
                </div>
            </div>
            <div class="modal-footer">
                <button on:click=move |_| show.set(false)>{move || t("jobs.close")}</button>
                <button class="primary" disabled=move || busy.get() on:click=on_submit>
                    {move || if busy.get() { t("jobs.submitting") } else { t("jobs.submit_btn") }}
                </button>
            </div>
        </Modal>
    }
}
