// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Worker drill-down: admission signals plus the task summaries this
//! worker currently owns.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::{fmt_count, fmt_short_duration, fmt_time};
use crate::i18n::{t, tf};
use crate::ui::{ErrorBanner, StateTag};
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_params_map;

#[component]
pub fn WorkerDetail() -> impl IntoView {
    let worker_id = use_params_map()
        .get_untracked()
        .get("id")
        .unwrap_or_default();

    let (detail, set_detail) = RwSignal::new_local(None::<api::WorkerDetail>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();

    let poll_id = worker_id.clone();
    use_polling(move || {
        let poll_id = poll_id.clone();
        spawn_local(async move {
            match api::worker_detail(&poll_id).await {
                Ok(value) => {
                    set_detail.set(Some(value));
                    set_error.set(None);
                    mark_refreshed();
                }
                Err(err) => set_error.set(Some(err)),
            }
        })
    });

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        {move || {
            detail
                .get()
                .map(|detail| {
                    let worker = detail.worker;
                    let load_pct = (worker.load_score_permille as f64 / 10.0).round() as i32;
                    let mem_pct = (worker.mem_permille as f64 / 10.0).round() as i32;
                    view! {
                        <div class="panel">
                            <h2>{tf("wd.title", &[&worker.worker_id.clone()])}" "<StateTag state=if worker.can_accept { "RUNNING".to_string() } else { "FAILED".to_string() } /></h2>
                            <div class="kv-grid">
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.address")}</div>
                                    <div class="kv-value mono">{worker.address.clone()}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.load")}</div>
                                    <div class="kv-value">{format!("{}%", load_pct)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.lag")}</div>
                                    <div class="kv-value">{format!("{} ms", worker.lag_ms)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.memory")}</div>
                                    <div class="kv-value">{format!("{}%", mem_pct)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.cpu")}</div>
                                    <div class="kv-value">{format!("{}%", worker.cpu_permille as f64 / 10.0)}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("cl.col.heartbeat")}</div>
                                    <div class="kv-value">{fmt_time(worker.last_heartbeat_ms)}</div>
                                </div>
                            </div>
                            <div class="hint"><a href="/cluster">{t("wd.back")}</a></div>
                        </div>
                        <div class="panel">
                            <h2>{t("wd.tasks")}</h2>
                            {if detail.tasks.is_empty() {
                                view! { <div class="muted">{t("wd.no_tasks")}</div> }.into_any()
                            } else {
                                view! {
                                    <table>
                                        <thead>
                                            <tr>
                                                <th>{t("jobs.col.job")}</th>
                                                <th>{t("jd.col.task_id")}</th>
                                                <th>{t("jobs.col.state")}</th>
                                                <th>{t("jd.col.processed")}</th>
                                                <th>{t("jd.col.throughput")}</th>
                                                <th>{t("jd.col.idle")}</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {detail
                                                .tasks
                                                .iter()
                                                .map(|task| {
                                                    let job_link = format!("/jobs/{}", task.job_id.clone());
                                                    view! {
                                                        <tr>
                                                            <td>
                                                                <a href=job_link>{task.job_name.clone()}</a>
                                                            </td>
                                                            <td class="mono">{task.task_id.clone()}</td>
                                                            <td><StateTag state=task.state.clone() /></td>
                                                            <td>{fmt_count(task.processed_records)}</td>
                                                            <td>
                                                                {if task.records_per_sec > 0.0 {
                                                                    format!("{:.1} rec/s", task.records_per_sec)
                                                                } else {
                                                                    "—".to_string()
                                                                }}
                                                            </td>
                                                            <td>
                                                                {if task.idle_ms < 0 {
                                                                    "—".to_string()
                                                                } else {
                                                                    fmt_short_duration(task.idle_ms)
                                                                }}
                                                            </td>
                                                        </tr>
                                                    }
                                                })
                                                .collect::<Vec<_>>()}
                                        </tbody>
                                    </table>
                                }
                                    .into_any()
                            }}
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
    }
}
