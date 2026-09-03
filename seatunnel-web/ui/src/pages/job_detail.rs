// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Job detail: basic info, task metrics (throughput/idle), live logs and
//! the edit/restart flows.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::charts::{LineChart, PALETTE, Series};
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
    // Live task logs arrive incrementally over the SSE stream.
    let logs = RwSignal::new_local(std::collections::BTreeMap::<String, Vec<String>>::new());
    let (stream_error, set_stream_error) = RwSignal::new_local(None::<String>).split();
    let (history, set_history) = RwSignal::new_local(None::<api::JobHistory>).split();
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
            match api::job_history(&poll_id).await {
                Ok(value) => set_history.set(Some(value)),
                Err(_) => set_history.set(None),
            }
        })
    });

    // Live logs stream: the console server tails the engine at ~1 s and
    // pushes per-task deltas; the stream reconnects (full snapshot) on its
    // own after the server-side lifetime cap. The handle lives in an Rc
    // cell (signals and streams are not Send in CSR) and is closed when
    // the component unmounts.
    {
        let handle_cell: std::sync::Mutex<Option<api::StreamHandle>> = std::sync::Mutex::new(None);
        let set_stream_error = set_stream_error.clone();
        match api::stream_job_logs(&job_id, move |event| {
            if let Some(err) = event.error {
                set_stream_error.set(Some(err));
                return;
            }
            set_stream_error.set(None);
            logs.update(|map| {
                let entry = map.entry(event.task_id).or_default();
                if event.reset {
                    *entry = event.lines;
                } else {
                    entry.extend(event.lines);
                    let excess = entry.len().saturating_sub(2000);
                    if excess > 0 {
                        entry.drain(..excess);
                    }
                }
            });
        }) {
            Ok(handle) => {
                *handle_cell.lock().unwrap() = Some(handle);
            }
            Err(err) => set_stream_error.set(Some(err)),
        }
        on_cleanup(move || {
            if let Some(handle) = handle_cell.lock().ok().and_then(|mut cell| cell.take()) {
                drop(handle);
            }
        });
    }

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
                                    <div class="kv-label">{t("jd.parallelism")}</div>
                                    <div class="kv-value">
                                        {if status.parallelism > 0 { status.parallelism.to_string() } else { "—".to_string() }}
                                    </div>
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
                                        <th>{t("jd.col.error")}</th>
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
                                                    <td class="mono small idle-critical" title={task.error.clone()}>
                                                        {if task.error.is_empty() {
                                                            "—".to_string()
                                                        } else {
                                                            task.error.clone()
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
        <MetricsCharts history=history />
        <TaskLogsPanel logs=logs stream_error=Signal::derive(move || stream_error.get()) />
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

/// Throughput and sink-latency charts fed by the console-side sampling
/// ring (one series per task).
#[component]
fn MetricsCharts(history: ReadSignal<Option<api::JobHistory>, LocalStorage>) -> impl IntoView {
    move || {
        history
            .get()
            .map(|history| {
                let rps = task_series(&history, |p| p.records_per_sec);
                let latency = task_series(&history, |p| p.latency_ema_ms);
                view! {
                    <div class="panel">
                        <h2>{t("jd.metrics")}</h2>
                        <LineChart title=t("chart.throughput") series=rps unit="rec/s" />
                        <LineChart title=t("chart.sink_latency") series=latency unit="ms" />
                    </div>
                }
                .into_any()
            })
            .unwrap_or_else(|| ().into_any())
    }
}

fn task_series(
    history: &api::JobHistory,
    pick: impl Fn(&api::TaskHistoryPoint) -> f64,
) -> Vec<Series> {
    let mut by_task: std::collections::BTreeMap<String, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    for point in &history.points {
        for task in &point.tasks {
            by_task
                .entry(task.task_id.clone())
                .or_default()
                .push((point.ts_ms as f64, pick(task)));
        }
    }
    by_task
        .into_iter()
        .enumerate()
        .map(|(i, (name, points))| Series {
            color: PALETTE[i % PALETTE.len()].to_string(),
            name,
            points,
        })
        .collect()
}

/// Live per-task log viewer fed by the SSE stream: per-task panes follow
/// the newest line until the user scrolls away; the whole panel can go
/// fullscreen (Esc exits).
#[component]
fn TaskLogsPanel(
    logs: leptos::prelude::RwSignal<
        std::collections::BTreeMap<String, Vec<String>>,
        leptos::prelude::LocalStorage,
    >,
    stream_error: Signal<Option<String>>,
) -> impl IntoView {
    let fs = RwSignal::new_local(false);
    crate::log_view::use_escape_on(move || fs.get(), move || fs.set(false));

    let total = move || logs.get().values().map(|v| v.len()).sum::<usize>();

    view! {
        <ErrorBanner message=stream_error />
        <div class="panel" class:logs-fs=move || fs.get()>
            <div class="log-head">
                <h2>{move || tf("jd.lines_title", &[&t("jd.live_logs"), &total().to_string()])}</h2>
                <button class="btn" on:click=move |_| fs.update(|value| *value = !*value)>
                    {move || if fs.get() { t("logs.exit_fs") } else { t("logs.fullscreen") }}
                </button>
            </div>
            {move || {
                let map = logs.get();
                if map.is_empty() {
                    view! { <div class="muted">{t("jd.no_logs")}</div> }.into_any()
                } else {
                    map
                        .into_iter()
                        .map(|(task_id, lines)| {
                            view! {
                                <div class="log-group">
                                    <div class="log-task mono">{task_id}</div>
                                    <crate::log_view::FollowLog content=Signal::derive(move || {
                                        lines.join("\n")
                                    }) />
                                </div>
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_any()
                }
            }}
        </div>
    }
}
