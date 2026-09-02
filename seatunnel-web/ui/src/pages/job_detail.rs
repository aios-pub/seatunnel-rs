// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job detail: basic info, task metrics (throughput/idle) and live logs.

use crate::api;
use crate::app::{poll_interval, RefreshControl};
use crate::fmt::{fmt_bytes, fmt_count, fmt_duration, fmt_short_duration, fmt_time};
use crate::ui::{ErrorBanner, StateTag};
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_params_map;
use wasm_bindgen::JsCast;

fn event_target_value(ev: &leptos::ev::Event) -> String {
    use wasm_bindgen::JsCast;
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
    // Edit-and-restart state: editor visibility, edited config, flow status.
    let (editor_open, set_editor_open) = RwSignal::new_local(false).split();
    let (editor_text, set_editor_text) = RwSignal::new_local(String::new()).split();
    let (updating, set_updating) = RwSignal::new_local(false).split();
    let (update_msg, set_update_msg) = RwSignal::new_local(None::<String>).split();
    let refresh = expect_context::<RefreshControl>();

    // Open the editor prefilled with the config exactly as submitted.
    let open_editor = {
        let job_id = job_id.clone();
        let set_editor_text = set_editor_text.clone();
        let set_editor_open = set_editor_open.clone();
        let set_update_msg = set_update_msg.clone();
        move |current_config: String| {
            let pretty = serde_json::from_str::<serde_json::Value>(&current_config)
                .and_then(|v| serde_json::to_string_pretty(&v))
                .unwrap_or(current_config);
            set_editor_text.set(pretty);
            set_update_msg.set(None);
            set_editor_open.set(true);
            let _ = &job_id;
        }
    };

    // Confirm: run the update flow (cancel → exit checkpoint → resubmit).
    // Reads the editor text from its signal at click time, so the handler
    // stays Copy (signal setters) and can be reused in closures freely.
    let confirm_update = {
        let job_id = job_id.clone();
        move || {
            let text = editor_text.get_untracked();
            let job_id = job_id.clone();
            set_updating.set(true);
            set_update_msg.set(Some(
                "Stopping the old incarnation (final checkpoint) and resubmitting…".to_string(),
            ));
            spawn_local(async move {
                let request = api::UpdateJobRequest {
                    config_text: text,
                    job_name: None,
                    parallelism: None,
                    cancel_timeout_secs: Some(60),
                };
                match api::update_job(&job_id, request).await {
                    Ok(result) => {
                        set_update_msg.set(Some(format!(
                            "Updated: {} (cancel took {} ms); the job restores from its latest checkpoint.",
                            result.message, result.cancel_wait_ms
                        )));
                        set_editor_open.set(false);
                    }
                    Err(err) => {
                        set_update_msg.set(Some(format!("Update failed: {}", err)));
                    }
                }
                set_updating.set(false);
            });
        }
    };

    // Restart-as-is: same id + the config retained at submission time; the
    // engine cancels a running incarnation first, then resubmits (tasks
    // restore from their latest checkpoint).
    let restart_as_is = {
        let job_id = job_id.clone();
        move || {
            let job_id = job_id.clone();
            set_updating.set(true);
            set_update_msg.set(Some(
                "Restarting with the retained config (cancelling the old incarnation first)…"
                    .to_string(),
            ));
            spawn_local(async move {
                match api::restart_job(&job_id).await {
                    Ok(result) => {
                        set_update_msg.set(Some(format!("Restarted: {}", result.message)));
                    }
                    Err(err) => {
                        set_update_msg.set(Some(format!("Restart failed: {}", err)));
                    }
                }
                set_updating.set(false);
            });
        }
    };

    let poll_id = job_id.clone();
    spawn_local(async move {
        loop {
            if refresh.0.get_untracked() {
                match api::job_status(&poll_id).await {
                    Ok(value) => {
                        set_status.set(Some(value));
                        set_error.set(None);
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
            }
            gloo_timers::future::TimeoutFuture::new(poll_interval()).await;
        }
    });

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        {move || {
            // Clone the shared handlers per invocation so the inner panel
            // closures capture locals, not this closure's environment.
            let confirm_update = confirm_update.clone();
            let open_editor = open_editor.clone();
            let restart_as_is = restart_as_is.clone();
            let set_editor_open = set_editor_open.clone();
            let set_editor_text = set_editor_text.clone();
            status
                .get()
                .map(|status| {
                    view! {
                        <div class="panel">
                            <h2>
                                {status.job_name.clone()}" "
                                <StateTag state=status.state.clone() />
                            </h2>
                            <div class="kv-grid">
                                <div class="kv">
                                    <div class="kv-label">"Job ID"</div>
                                    <div class="kv-value mono">{status.job_id.clone()}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Started"</div>
                                    <div class="kv-value">{fmt_time(status.start_time_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Duration"</div>
                                    <div class="kv-value">{fmt_duration(status.start_time_ms, status.end_time_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Checkpoint interval"</div>
                                    <div class="kv-value">{format!("{} ms", status.checkpoint_interval_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Checkpoints completed"</div>
                                    <div class="kv-value">{fmt_count(status.checkpoints_completed)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Tasks"</div>
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
                                        move |_| {
                                            open_editor(config.clone());
                                        }
                                    }
                                >
                                    {move || {
                                        if updating.get() {
                                            "Updating…".to_string()
                                        } else if status.state == "RUNNING" {
                                            "编辑配置并重启 (Edit & restart)".to_string()
                                        } else {
                                            "以同 ID 重新提交 (Resubmit same id)".to_string()
                                        }
                                    }}
                                </button>
                                <button
                                    class="btn"
                                    disabled=move || updating.get()
                                    on:click={
                                        let restart_as_is = restart_as_is.clone();
                                        move |_| restart_as_is()
                                    }
                                >
                                    {move || {
                                        if updating.get() {
                                            "Restarting…".to_string()
                                        } else {
                                            "重启 (Restart)".to_string()
                                        }
                                    }}
                                </button>
                                {move || {
                                    update_msg
                                        .get()
                                        .map(|m| view! { <span class="update-msg">{m}</span> })
                                }}
                            </div>
                            <div class="hint">
                                "Update = cancel (automatic exit checkpoint) then resubmit with the SAME job id: workers resume from the latest checkpoint (at-least-once; exactly-once with transactional sinks). Cross-worker restore requires s3/master checkpoint storage. Restart = same flow with the ORIGINAL config, no editing."
                            </div>
                            {(!status.error_message.is_empty()).then(|| {
                                view! {
                                    <div class="error-banner" style="margin: 12px 0 0;">
                                        {status.error_message.clone()}
                                    </div>
                                }
                            })}
                        </div>
                        {move || {
                            if editor_open.get() {
                                let confirm = confirm_update.clone();
                                let set_editor_open = set_editor_open.clone();
                                view! {
                                    <div class="panel editor-panel">
                                        <h2>"Edit job configuration (JSON)"</h2>
                                        <textarea
                                            class="config-editor"
                                            prop:value=move || editor_text.get()
                                            on:input={
                                                let set_editor_text = set_editor_text.clone();
                                                move |ev| {
                                                    set_editor_text.set(event_target_value(&ev));
                                                }
                                            }
                                        />
                                        <div class="job-actions">
                                            <button
                                                class="btn primary"
                                                disabled=move || updating.get()
                                                on:click={
                                                    let confirm = confirm.clone();
                                                    move |_| confirm()
                                                }
                                            >
                                                "确认更新并重启"
                                            </button>
                                            <button
                                                class="btn"
                                                disabled=move || updating.get()
                                                on:click={
                                                    let set_editor_open = set_editor_open.clone();
                                                    move |_| set_editor_open.set(false)
                                                }
                                            >
                                                "取消"
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
                            <h2>"Tasks"</h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>"Task ID"</th>
                                        <th>"Worker"</th>
                                        <th>"State"</th>
                                        <th>"Processed"</th>
                                        <th>"Throughput"</th>
                                        <th>"Idle"</th>
                                        <th>"Sink Delivery"</th>
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
                .unwrap_or_else(|| view! { <div class="loading">"Loading…"</div> }.into_any())
        }}
        <TaskLogsPanel logs=logs />
        {move || {
            checkpoints
                .get()
                .map(|history| {
                    view! {
                        <div class="panel">
                            <h2>
                                {format!(
                                    "Checkpoint history — {} completed, every {} ms",
                                    crate::fmt::fmt_count(history.checkpoints_completed),
                                    history.checkpoint_interval_ms,
                                )}
                            </h2>
                            {if history.tasks.is_empty() {
                                view! { <div class="muted">"No checkpoints recorded yet (waiting for the first interval to complete)."</div> }.into_any()
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
                                <h2>"Live logs ("{total.to_string()}" lines)"</h2>
                                <label class="log-autoscroll">
                                    <input
                                        type="checkbox"
                                        prop:checked=move || auto_scroll.get()
                                        on:change=move |event| {
                                            auto_scroll.set(event_target_checked(&event));
                                        }
                                    />
                                    " auto-scroll"
                                </label>
                            </div>
                            {if total == 0 {
                                view! { <div class="muted">"No task logs yet."</div> }.into_any()
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
