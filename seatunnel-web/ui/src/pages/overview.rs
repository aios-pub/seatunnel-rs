// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Dashboard: job counts, cluster summary and per-state breakdown.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::fmt_count;
use crate::i18n::t;
use crate::ui::{ErrorBanner, StatCard};
use leptos::prelude::*;
use leptos::task::spawn_local;

#[component]
pub fn Overview() -> impl IntoView {
    let (data, set_data) = RwSignal::new_local(None::<api::Overview>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();

    use_polling(move || {
        spawn_local(async move {
            match api::overview().await {
                Ok(value) => {
                    set_data.set(Some(value));
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
            data.get()
                .map(|overview| {
                    let cluster = &overview.cluster;
                    view! {
                        <div class="cards">
                            <StatCard label=t("ov.running_jobs") value=fmt_count(overview.jobs_running) tone="running" />
                            <StatCard label=t("ov.pending") value=fmt_count(overview.jobs_pending) tone="muted" />
                            <StatCard label=t("ov.completed") value=fmt_count(overview.jobs_completed) tone="completed" />
                            <StatCard label=t("ov.failed") value=fmt_count(overview.jobs_failed) tone="failed" />
                            <StatCard label=t("ov.cancelled") value=fmt_count(overview.jobs_cancelled) tone="muted" />
                            <StatCard label=t("ov.total_jobs") value=fmt_count(overview.jobs_total) />
                            <StatCard label=t("ov.workers") value=fmt_count(cluster.available_workers as i64) />
                            <StatCard label=t("ov.running_tasks") value=fmt_count(cluster.running_tasks as i64) tone="running" />
                        </div>
                        <div class="panel">
                            <h2>{t("ov.cluster")}</h2>
                            <div class="kv-grid">
                                <div class="kv">
                                    <div class="kv-label">{t("ov.leader")}</div>
                                    <div class="kv-value mono">{cluster.leader_id.clone()}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("ov.workers")}</div>
                                    <div class="kv-value">{cluster.available_workers}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">{t("ov.tasks_total_running")}</div>
                                    <div class="kv-value">{format!("{} / {}", cluster.total_tasks, cluster.running_tasks)}</div>
                                </div>
                            </div>
                        </div>
                        <div class="panel">
                            <h2>{t("ov.jobs_by_state")}</h2>
                            <table>
                                <thead>
                                    <tr><th>{t("ov.state")}</th><th>{t("ov.jobs")}</th></tr>
                                </thead>
                                <tbody>
                                    {overview
                                        .jobs_by_state
                                        .iter()
                                        .map(|(state, count)| {
                                            view! {
                                                <tr>
                                                    <td><crate::ui::StateTag state=state.clone() /></td>
                                                    <td>{fmt_count(*count)}</td>
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
    }
}
