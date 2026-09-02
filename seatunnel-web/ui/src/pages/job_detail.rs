// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job detail: basic info, task metrics (throughput/idle), live logs and
//! the edit/restart flows.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::{fmt_bytes, fmt_count, fmt_duration, fmt_short_duration, fmt_time};
use crate::i18n::{lang, t, tf};
use crate::ui::{push_toast, ConfirmDialog, ErrorBanner, StateTag, ToastKind};
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_params_map;
use wasm_bindgen::JsCast;

fn textarea_value(ev: &leptos::ev::Event) -> String {
    ev.target()
        .and_then(|t| t.dyn_into::<web_sys::HtmlTextAreaElement>().ok())
        .map(|t| t.value())
        .unwrap_or_default()
}

#[component]
pub fn JobDetail() -> impl IntoView {
    // The component remounts on route change, so the id is read once.
    let job_id = use_params_map()
        .get_untracked()
        .get("id")
        .unwrap_or_default();

    let (status, set_status) = RwSignal::new_local(None::<api::JobStatus>).split();
    let (checkpoints, set_checkpoints) = RwSignal::new_local(None::<api::CheckpointHistory>).split();
    let (logs, set_logs) = RwSignal::new_local(None::<api::JobLogs>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    // Edit-and-restart state: editor visibility, edited config, flow busy flag.
    let (editor_open, set_editor_open) = RwSignal::new_local(false).split();
    let (editor_text, set_editor_text) = RwSignal::new_local(String::new()).split();
    let (updating, set_updating) = RwSignal::new_local(false).split();
    // Confirmation dialogs for the two destructive flows.
    let confirm_restart = RwSignal::new(false);
    let confirm_edit = RwSignal::new(false);

    let poll_id = job_id.clone();
    use_polling(move || {
        let poll_id = poll_id.clone();
        spawn_local(async move {
            match api::job_status(&poll_id).await {
                Ok(value) => {
                    set_status.set(Some(value));
                    set_error.set(None);
                    mark_refreshed();
                }
                Err(err) => set_error.set(Some(err)),
            }
            match api::job_checkpoints(&poll_id).await {
                Ok(value) => set_checkpoints.set(Some(value)),
                Err(_) => set_checkpoints.set(None),
            }
            match api::job_logs(&poll_id).await {
                Ok(value) => set_logs.set(Some(value)),
                Err(_) => set_logs.set(None),
            }
        })
    });

    // Edit flow: cancel (exit checkpoint) → resubmit with the edited config.
    // Reads the editor text from its signal at click time.
    let run_update = {
        let job_id = job_id.clone();
        Callback::new(move |_| {
            let text = editor_text.get_untracked();
            let job_id = job_id.clone();
            set_updating.set(true);
            push_toast(ToastKind::Info, t("jd.update_running"));
            spawn_local(async move {
                let request = api::UpdateJobRequest {
                    config_text: text,
                    job_name: None,
                    parallelism: None,
                    cancel_timeout_secs: Some(60),
                };
                match api::update_job(&job_id, request).await {
                    Ok(result) => {
                        push_toast(
                            ToastKind::Success,
                            tf(
                                "jd.updated",
                                &[&result.message, &result.cancel_wait_ms.to_string()],
                            ),
                        );
                        set_editor_open.set(false);
                    }
                    Err(err) => push_toast(ToastKind::Error, tf("jd.update_failed", &[&err])),
                }
                set_updating.set(false);
            });
        })
    };

    // Restart-as-is: same id + the config retained at submission time.
    let run_restart = {
        let job_id = job_id.clone();
        Callback::new(move |_| {
            let job_id = job_id.clone();
            set_updating.set(true);
            push_toast(ToastKind::Info, t("jd.restart_started"));
            spawn_local(async move {
                match api::restart_job(&job_id).await {
                    Ok(result) => {
                        push_toast(ToastKind::Success, tf("jd.restarted", &[&result.message]))
                    }
                    Err(err) => push_toast(ToastKind::Error, tf("jd.restart_failed", &[&err])),
                }
                set_updating.set(false);
            });
        })
    };

    // Open the editor prefilled with the config exactly as submitted.
    let open_editor = {
        let set_editor_text = set_editor_text.clone();
        let set_editor_open = set_editor_open.clone();
        move |current_config: String| {
            let pretty = serde_json::from_str::<serde_json::Value>(&current_config)
                .and_then(|v| serde_json::to_string_pretty(&v))
                .unwrap_or(current_config);
            set_editor_text.set(pretty);
            set_editor_open.set(true);
        }
    };

    let restart_message =
        Signal::derive(move || tf("jd.restart_confirm", &[job_id.as_str()]));
    let edit_message = Signal::derive(move || t("jd.edit_restart_confirm"));

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        {move || {
            status
                .get()
                .map(|status| {
                    let open_editor = open_editor.clone();
                    let confirm_restart = confirm_restart.clone();
                    let confirm_edit = confirm_edit.clone();
                    // Hoisted so the inner view closures capture plain
                    // values instead of borrowing `status`.
                    let is_running = status.state == "RUNNING";
                    let job_error = status.error_message.clone();
                    view! {
                        <div class="panel">
                            <h2>
                                {status.job_name.clone()}" "
                                <StateTag state=status.state.clone() />
                            </h2>
                            <div class="kv-grid">
                                <div class="kv">
                                    <div class="kv-label">{t("jd.job_id")}</div>
                                    <div class="kv-value mono">{status.job_id.clone()}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("jd.started")}</div>
                                    <div class="kv-value">{fmt_time(status.start_time_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("jd.duration")}</div>
                                    <div class="kv-value">{fmt_duration(status.start_time_ms, status.end_time_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("jd.cp_interval")}</div>
                                    <div class="kv-value">{format!("{} ms", status.checkpoint_interval_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("jd.cp_completed")}</div>
                                    <div class="kv-value">{fmt_count(status.checkpoints_completed)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("jd.tasks")}</div>
                                    <div class="kv-value">{status.tasks.len()}</div>
                                </div>
                            </div>
                            <div class="job-actions">
                                <button
                                    class="btn"
                                    disabled=move || updating.get()
                                    on:click={
                                        let open_editor = open_editor.clone();
                                        let config = status.job_config.clone();
                                        move |_| open_editor(config.clone())
                                    }
                                >
                                    {move || {
                                        if updating.get() {
                                            t("jd.updating")
                                        } else if is_running {
                                            t("jd.edit_restart")
                                        } else {
                                            t("jd.resubmit")
                                        }
                                    }}
                                </button>
                                <button
                                    class="btn"
                                    disabled=move || updating.get()
                                    on:click=move |_| confirm_restart.set(true)
                                >
                                    {move || {
                                        if updating.get() {
                                            t("jd.restarting")
                                        } else {
                                            t("jd.restart")
                                        }
                                    }}
                                </button>
                            </div>
                            <div class="hint">{t("jd.hint")}</div>
                            {(!job_error.is_empty()).then(|| {
                                view! {
                                    <div class="error-banner" style="margin: 12px 0 0;">
                                        {job_error.clone()}
                                    </div>
                                }
                            })}
                        </div>
                        {move || {
                            if editor_open.get() {
                                view! {
                                    <div class="panel editor-panel">
                                        <h2>{t("jd.editor_title")}</h2>
                                        <textarea
                                            class="config-editor"
                                            prop:value=move || editor_text.get()
                                            on:input=move |ev| set_editor_text.set(textarea_value(&ev))
                                        />
                                        <div class="job-actions">
                                            <button
                                                class="btn primary"
                                                disabled=move || updating.get()
                                                on:click=move |_| confirm_edit.set(true)
                                            >
                                                {t("jd.confirm_update")}
                                            </button>
                                            <button
                                                class="btn"
                                                on:click=move |_| set_editor_open.set(false)
                                            >
                                                {t("misc.cancel")}
                                            </button>
                                        </div>
                                    </div>
                                }
                                .into_any()
                            } else {
                                ().into_any()
                            }
                        }}
                        <div class="panel">
                            <h2>{t("jd.tasks")}</h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>{t("jd.col.task_id")}</th>
                                        <th>{t("jd.col.worker")}</th>
                                        <th>{t("jobs.col.state")}</th>
                                        <th>{t("jd.col.processed")}</th>
                                        <th>{t("jd.col.throughput")}</th>
                                        <th>{t("jd.col.idle")}</th>
                                        <th>{t("jd.col.sink")}</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {status
                                        .tasks
                                        .iter()
                                        .map(|task| {
                                            let idle_class = match task.idle_ms {
                                                d if d < 0 => "muted",
                                                d if d > 30_000 => "idle-critical",
                                                d if d > 10_000 => "idle-warn",
                                                _ => "idle-ok",
                                            };
                                            let throughput = if task.records_per_sec > 0.0 {
                                                format!("{:.1} rec/s", task.records_per_sec)
                                            } else {
                                                "—".to_string()
                                            };
                                            view! {
                                                <tr>
                                                    <td class="mono">{task.task_id.clone()}</td>
                                                    <td class="mono">
                                                        {if task.worker_id.is_empty() { "—".to_string() } else { task.worker_id.clone() }}
                                                    </td>
                                                    <td><StateTag state=task.state.clone() /></td>
                                                    <td>{fmt_count(task.processed_records)}</td>
                                                    <td>{throughput}</td>
                                                    <td class=idle_class>
                                                        {if task.idle_ms < 0 { "—".to_string() } else { fmt_short_duration(task.idle_ms) }}
                                                    </td>
                                                    <td class="mono small">
                                                        {match &task.sink_metrics {
                                                            None => "—".to_string(),
                                                            Some(m) => {
                                                                let failed = if m.failed > 0 {
                                                                    format!("  ✗{}", m.failed)
                                                                } else {
                                                                    String::new()
                                                                };
                                                                format!(
                                                                    "{:.0}ms  ⇄{}  {}/{}{}",
                                                                    m.latency_ema_ms,
                                                                    m.in_flight,
                                                                    m.delivered,
                                                                    m.window_secs,
                                                                    failed,
                                                                )
                                                            }
                                                        }}
                                                    </td>
                                                </tr>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tbody>
                            </table>
                        </div>
                    }
                    .into_any()
                })
                .unwrap_or_else(|| {
                    if error.get().is_some() {
                        view! { <div class="muted">{t("misc.no_data")}</div> }.into_any()
                    } else {
                        view! { <div class="loading">{t("misc.loading")}</div> }.into_any()
                    }
                })
        }}
        <TaskLogsPanel logs=logs />
        {move || {
            checkpoints
                .get()
                .map(|history| {
                    view! {
                        <div class="panel">
                            <h2>
                                {tf(
                                    "jd.cp_history",
                                    &[&fmt_count(history.checkpoints_completed), &history.checkpoint_interval_ms.to_string()],
                                )}
                            </h2>
                            {if history.tasks.is_empty() {
                                view! { <div class="muted">{t("jd.no_checkpoints")}</div> }.into_any()
                            } else {
                                history
                                    .tasks
                                    .iter()
                                    .map(|task| {
                                        view! {
                                            <div class="field">
                                                <label class="mono">{task.task_id.clone()}</label>
                                                <div class="checkpoint-list">
                                                    {task
                                                        .entries
                                                        .iter()
                                                        .rev()
                                                        .map(|entry| {
                                                            view! {
                                                                <span class="checkpoint-chip">
                                                                    {format!("#{} ({})", entry.checkpoint_id, fmt_bytes(entry.size_bytes))}
                                                                </span>
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()}
                                                </div>
                                            </div>
                                        }
                                    })
                                    .collect::<Vec<_>>()
                                    .into_any()
                            }}
                        </div>
                    }
                    .into_any()
                })
                .unwrap_or_else(|| ().into_any())
        }}
        {move || {
            let _ = lang().get();
            let title = t("jd.restart");
            view! {
                <ConfirmDialog
                    show=confirm_restart
                    title=title
                    message=restart_message
                    confirm_label=t("jd.restart")
                    danger=true
                    on_confirm=run_restart.clone()
                />
            }
        }}
        {move || {
            let _ = lang().get();
            let title = t("jd.confirm_update");
            view! {
                <ConfirmDialog
                    show=confirm_edit
                    title=title
                    message=edit_message
                    confirm_label=t("jd.confirm_update")
                    danger=true
                    on_confirm=run_update.clone()
                />
            }
        }}
    }
}

/// Live per-task log viewer: lifecycle events and sampled data rows.
/// Auto-scrolls every log box to the newest line on each refresh; the
/// toggle lets the user pin the view while reading history.
#[component]
fn TaskLogsPanel(
    logs: leptos::prelude::ReadSignal<Option<api::JobLogs>, leptos::prelude::LocalStorage>,
) -> impl IntoView {
    let auto_scroll = RwSignal::new(true);

    // Runs after the DOM patch for each log refresh; scrolling here keeps
    // the newest lines in view.
    Effect::new(move || {
        logs.get();
        if !auto_scroll.get() {
            return;
        }
        let Some(doc) = window().document() else { return };
        let boxes = doc.query_selector_all(".log-box").unwrap();
        for index in 0..boxes.length() {
            if let Some(node) = boxes.item(index) {
                let element: web_sys::HtmlElement = node.unchecked_into();
                element.set_scroll_top(element.scroll_height());
            }
        }
    });

    view! {
        {move || {
            logs.get()
                .map(|logs| {
                    let total: usize = logs.tasks.iter().map(|t| t.lines.len()).sum();
                    view! {
                        <div class="panel">
                            <div class="log-head">
                                <h2>{tf("jd.lines_title", &[&t("jd.live_logs"), &total.to_string()])}</h2>
                                <label class="log-autoscroll">
                                    <input
                                        type="checkbox"
                                        prop:checked=move || auto_scroll.get()
                                        on:change=move |event| {
                                            auto_scroll.set(event_target_checked(&event));
                                        }
                                    />
                                    {t("jd.autoscroll")}
                                </label>
                            </div>
                            {if total == 0 {
                                view! { <div class="muted">{t("jd.no_logs")}</div> }.into_any()
                            } else {
                                logs
                                    .tasks
                                    .iter()
                                    .map(|task| {
                                        view! {
                                            <div class="log-group">
                                                <div class="log-task mono">{task.task_id.clone()}</div>
                                                <pre class="log-box">{
                                                    task.lines
                                                        .iter()
                                                        .rev()
                                                        .take(200)
                                                        .rev()
                                                        .cloned()
                                                        .collect::<Vec<_>>()
                                                        .join("\n")
                                                }</pre>
                                            </div>
                                        }
                                    })
                                    .collect::<Vec<_>>()
                                    .into_any()
                            }}
                        </div>
                    }
                    .into_any()
                })
                .unwrap_or_else(|| ().into_any())
        }}
    }
}
