// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job list: filtering, search, sorting, pagination, batch stop, submit
//! dialog (config text or file) and per-row stop/delete actions.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::{fmt_duration, fmt_time};
use crate::i18n::{lang, t, tf};
use crate::ui::{push_toast, ConfirmDialog, ErrorBanner, Modal, StateTag, ToastKind};
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_navigate;
use std::collections::HashSet;
use wasm_bindgen::JsCast;

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

const PAGE_SIZE: usize = 50;

fn is_terminal_state(state: &str) -> bool {
    matches!(state, "COMPLETED" | "FAILED" | "CANCELLED")
}

#[component]
pub fn Jobs() -> impl IntoView {
    let (jobs, set_jobs) = RwSignal::new_local(Vec::<api::JobSummary>::new()).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();

    // Filter / sort / pagination controls (all client-side).
    let (state_filter, set_state_filter) = RwSignal::new_local(String::new()).split();
    let (search, set_search) = RwSignal::new_local(String::new()).split();
    let (sort, set_sort) = RwSignal::new_local(0usize).split();
    let (page, set_page) = RwSignal::new_local(0usize).split();
    let (selected, set_selected) = RwSignal::new_local(HashSet::<String>::new()).split();
    let (batch_open, set_batch_open) = {
        let signal = RwSignal::new(false);
        (signal, signal)
    };

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

    // States present in the data, for the filter dropdown.
    let known_states = move || {
        let mut states: Vec<String> = jobs
            .get()
            .iter()
            .map(|j| j.state.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        states.sort();
        states
    };

    // Filtered / searched / sorted view plus pagination bounds:
    // (rows on the current page, total matching, first row index, page).
    let view_rows = move || {
        let needle = search.get().to_lowercase();
        let filter = state_filter.get();
        let sort = sort.get();

        let mut rows: Vec<api::JobSummary> = jobs
            .get()
            .into_iter()
            .filter(|job| {
                (filter.is_empty() || job.state == filter)
                    && (needle.is_empty()
                        || job.job_name.to_lowercase().contains(&needle)
                        || job.job_id.to_lowercase().contains(&needle))
            })
            .collect();
        match sort {
            1 => rows.sort_by_key(|j| j.start_time_ms),
            2 => rows.sort_by(|a, b| a.job_name.to_lowercase().cmp(&b.job_name.to_lowercase())),
            3 => {
                rows.sort_by_key(|j| {
                    let end = if j.end_time_ms > 0 {
                        j.end_time_ms
                    } else {
                        js_sys::Date::now() as i64
                    };
                    -(end - j.start_time_ms)
                });
            }
            _ => rows.sort_by_key(|j| -j.start_time_ms),
        }

        let total = rows.len();
        let current = page.get().min(total.saturating_sub(1) / PAGE_SIZE);
        let start = current * PAGE_SIZE;
        let page_rows: Vec<api::JobSummary> =
            rows.into_iter().skip(start).take(PAGE_SIZE).collect();
        (page_rows, total, start, current)
    };

    // Count of selected jobs that are still running (batch-stop targets).
    let running_selected_count = move || {
        let sel = selected.get();
        jobs.get()
            .iter()
            .filter(|j| sel.contains(&j.job_id) && !is_terminal_state(&j.state))
            .count()
    };

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        <div class="toolbar">
            <button class="primary" on:click=move |_| show_submit.set(true)>
                {move || t("jobs.submit")}
            </button>
            <span class="muted">{move || tf("jobs.count", &[&jobs.get().len().to_string()])}</span>
            <div class="toolbar-gap"></div>
            {move || {
                let count = running_selected_count();
                (count > 0).then(|| {
                    view! {
                        <button class="danger" on:click=move |_| set_batch_open.set(true)>
                            {tf("jobs.batch_stop", &[&count.to_string()])}
                        </button>
                    }
                })
            }}
        </div>
        <div class="toolbar">
            <select
                class="inline-select"
                on:change=move |event| {
                    set_state_filter.set(event_target_value(&event));
                    set_page.set(0);
                }
            >
                <option value="">{move || t("jobs.filter_all")}</option>
                {move || {
                    known_states()
                        .into_iter()
                        .map(|state| {
                            let value = state.clone();
                            view! { <option value=value>{state}</option> }
                        })
                        .collect::<Vec<_>>()
                }}
            </select>
            <input
                class="inline-search"
                type="search"
                placeholder={move || t("jobs.search_ph")}
                prop:value=move || search.get()
                on:input=move |event| {
                    set_search.set(event_target_value(&event));
                    set_page.set(0);
                }
            />
            <select
                class="inline-select"
                on:change=move |event| {
                    set_sort.set(event_target_value(&event).parse().unwrap_or(0));
                }
            >
                <option value="0">{move || t("jobs.sort.newest")}</option>
                <option value="1">{move || t("jobs.sort.oldest")}</option>
                <option value="2">{move || t("jobs.sort.name")}</option>
                <option value="3">{move || t("jobs.sort.duration")}</option>
            </select>
        </div>
        <div class="panel">
            <table>
                <thead>
                    <tr>
                        <th class="col-check">
                            {move || {
                                let (page_rows, _, _, _) = view_rows();
                                let all_checked = !page_rows.is_empty()
                                    && page_rows
                                        .iter()
                                        .filter(|j| !is_terminal_state(&j.state))
                                        .all(|j| selected.get().contains(&j.job_id));
                                (!page_rows.is_empty()).then(move || {
                                    view! {
                                        <input
                                            type="checkbox"
                                            title=move || t("jobs.select_all")
                                            prop:checked=move || all_checked
                                            on:change=move |event| {
                                                let checked = event_target_checked(&event);
                                                let (page_rows, _, _, _) = view_rows();
                                                set_selected.update(|sel| {
                                                    for job in &page_rows {
                                                        if !is_terminal_state(&job.state) {
                                                            if checked {
                                                                sel.insert(job.job_id.clone());
                                                            } else {
                                                                sel.remove(&job.job_id);
                                                            }
                                                        }
                                                    }
                                                });
                                            }
                                        />
                                    }
                                })
                            }}
                        </th>
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
                        let (page_rows, ..) = view_rows();
                        page_rows
                            .into_iter()
                            .map(|job| {
                                let job_id = job.job_id.clone();
                                let cancel_id = job.job_id.clone();
                                let delete_id = job.job_id.clone();
                                let link_id = job.job_id.clone();
                                let state = job.state.clone();
                                let navigate = navigate.clone();
                                let terminal = is_terminal_state(&state);
                                let row_id = job.job_id.clone();
                                view! {
                                    <tr>
                                        <td class="col-check">
                                            {(!terminal).then(move || {
                                                let for_checked = row_id.clone();
                                                let for_change = row_id.clone();
                                                view! {
                                                    <input
                                                        type="checkbox"
                                                        prop:checked=move || selected.get().contains(&for_checked)
                                                        on:change=move |event| {
                                                            let checked = event_target_checked(&event);
                                                            let id = for_change.clone();
                                                            set_selected.update(|sel| {
                                                                if checked { sel.insert(id); } else { sel.remove(&id); }
                                                            });
                                                        }
                                                    />
                                                }
                                            })}
                                        </td>
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
                                        <td><StateTag state=state.clone() /></td>
                                        <td>{fmt_time(job.start_time_ms)}</td>
                                        <td>{fmt_duration(job.start_time_ms, job.end_time_ms)}</td>
                                        <td>
                                            {if terminal {
                                                view! { <DeleteJobButton job_id=delete_id /> }.into_any()
                                            } else {
                                                view! { <CancelJobButton job_id=cancel_id /> }.into_any()
                                            }}
                                        </td>
                                    </tr>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tbody>
            </table>
            {move || {
                let (_, total, start, current) = view_rows();
                (total > PAGE_SIZE).then(move || {
                    let from = start + 1;
                    let to = (start + PAGE_SIZE).min(total);
                    view! {
                        <div class="pager">
                            <button
                                disabled=current == 0
                                on:click=move |_| set_page.update(|p| *p = p.saturating_sub(1))
                            >
                                {t("jobs.prev")}
                            </button>
                            <span class="muted">
                                {tf("jobs.page_info", &[&from.to_string(), &to.to_string(), &total.to_string()])}
                            </span>
                            <button
                                disabled=(current + 1) * PAGE_SIZE >= total
                                on:click=move |_| set_page.update(|p| *p += 1)
                            >
                                {t("jobs.next")}
                            </button>
                        </div>
                    }
                })
            }}
        </div>
        <SubmitJobDialog show=show_submit />
        {move || {
            let _ = lang().get();
            let count = running_selected_count().to_string();
            let count_msg = count.clone();
            let ids: Vec<String> = selected.get().into_iter().collect();
            view! {
                <ConfirmDialog
                    show=batch_open
                    title=t("jobs.cancel")
                    message=Signal::derive(move || tf("jobs.batch_stop_confirm", &[&count_msg]))
                    confirm_label=tf("jobs.batch_stop", &[&count])
                    danger=true
                    on_confirm=Callback::new(move |_| {
                        let ids = ids.clone();
                        spawn_local(async move {
                            let mut stopped = 0usize;
                            for id in &ids {
                                if api::cancel_job(id).await.is_ok() {
                                    stopped += 1;
                                }
                            }
                            set_selected.set(HashSet::new());
                            push_toast(
                                ToastKind::Success,
                                tf("jobs.batch_stopped", &[&stopped.to_string()]),
                            );
                        });
                    })
                />
            }
        }}
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

/// Delete button for terminal jobs: removes the history record (state +
/// checkpoint metadata) after confirmation.
#[component]
fn DeleteJobButton(job_id: String) -> impl IntoView {
    let busy = RwSignal::new(false);
    let confirm_open = RwSignal::new(false);
    let message = Signal::derive({
        let job_id = job_id.clone();
        move || tf("jobs.delete_confirm", &[job_id.as_str()])
    });
    let del = {
        let job_id = job_id.clone();
        Callback::new(move |_| {
            busy.set(true);
            let job_id = job_id.clone();
            spawn_local(async move {
                match api::delete_job(&job_id).await {
                    Ok(()) => {
                        push_toast(ToastKind::Success, tf("jobs.deleted", &[&job_id]));
                    }
                    Err(err) => {
                        push_toast(
                            ToastKind::Error,
                            format!("{}: {}", t("jobs.delete_failed"), err),
                        )
                    }
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
        >{move || t("jobs.delete")}</button>
        {move || {
            let _ = lang().get();
            let title = tf("jobs.delete_title", &[job_id.as_str()]);
            view! {
                <ConfirmDialog
                    show=confirm_open
                    title=title
                    message=message
                    confirm_label=t("jobs.delete")
                    danger=true
                    on_confirm=del.clone()
                />
            }
        }}
    }
}

/// Job submission dialog: config text (or file) + format + name +
/// parallelism, with a client-side JSON pre-check.
#[component]
fn SubmitJobDialog(show: RwSignal<bool>) -> impl IntoView {
    let config_text = RwSignal::new(DEFAULT_CONFIG.to_string());
    let format = RwSignal::new("yaml".to_string());
    let job_name = RwSignal::new(String::new());
    let parallelism = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let (error, set_error) = RwSignal::new_local(None::<String>).split();

    let on_file = move |ev: leptos::ev::Event| {
        let Some(input) = ev
            .target()
            .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
        else {
            return;
        };
        let Some(file) = input.files().and_then(|files| files.get(0)) else {
            return;
        };
        let name = file.name().to_lowercase();
        let detected = if name.ends_with(".json") {
            "json"
        } else if name.ends_with(".toml") {
            "toml"
        } else if name.ends_with(".conf") {
            "hocon"
        } else {
            "yaml"
        };
        let text = wasm_bindgen_futures::JsFuture::from(file.text());
        spawn_local(async move {
            if let Ok(value) = text.await {
                if let Some(content) = value.as_string() {
                    config_text.set(content);
                    format.set(detected.to_string());
                }
            }
        });
    };

    let on_submit = move |_| {
        if busy.get_untracked() {
            return;
        }
        let chosen_format = format.get_untracked();
        // Pre-check JSON client-side so obvious syntax errors never leave
        // the browser.
        if chosen_format == "json" {
            if let Err(err) =
                serde_json::from_str::<serde_json::Value>(&config_text.get_untracked())
            {
                set_error.set(Some(format!("{} ({})", t("jobs.invalid_json"), err)));
                return;
            }
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
            format: chosen_format,
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
            <div class="field">
                <label>{move || t("jobs.file")}</label>
                <input type="file" accept=".yaml,.yml,.json,.toml,.conf" on:change=on_file />
            </div>
            <div class="form-row">
                <div class="field">
                    <label>{move || t("jobs.format")}</label>
                    <select on:change=move |event| format.set(event_target_value(&event))>
                        <option value="yaml" selected=true>"yaml"</option>
                        <option value="toml">"toml"</option>
                        <option value="hocon">"hocon"</option>
                        <option value="json">"json"</option>
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
