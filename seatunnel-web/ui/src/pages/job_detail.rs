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
    let refresh = expect_context::<RefreshControl>();

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
                            {(!status.error_message.is_empty()).then(|| {
                                view! {
                                    <div class="error-banner" style="margin: 12px 0 0;">
                                        {status.error_message.clone()}
                                    </div>
                                }
                            })}
                        </div>
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
