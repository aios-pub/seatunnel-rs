// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Dashboard: job counts, cluster summary and per-state breakdown.

use crate::api;
use crate::app::{poll_interval, RefreshControl};
use crate::fmt::fmt_count;
use crate::ui::{ErrorBanner, StatCard};
use leptos::prelude::*;
use leptos::task::spawn_local;

#[component]
pub fn Overview() -> impl IntoView {
    let (data, set_data) = RwSignal::new_local(None::<api::Overview>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let refresh = expect_context::<RefreshControl>();

    spawn_local(async move {
        loop {
            if refresh.0.get_untracked() {
                match api::overview().await {
                    Ok(value) => {
                        set_data.set(Some(value));
                        set_error.set(None);
                    }
                    Err(err) => set_error.set(Some(err)),
                }
            }
            gloo_timers::future::TimeoutFuture::new(poll_interval()).await;
        }
    });

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        {move || {
            data.get()
                .map(|overview| {
                    let cluster = &overview.cluster;
                    view! {
                        <div class="cards">
                            <StatCard label="Running jobs" value=fmt_count(overview.jobs_running) tone="running" />
                            <StatCard label="Pending" value=fmt_count(overview.jobs_pending) tone="muted" />
                            <StatCard label="Completed" value=fmt_count(overview.jobs_completed) tone="completed" />
                            <StatCard label="Failed" value=fmt_count(overview.jobs_failed) tone="failed" />
                            <StatCard label="Cancelled" value=fmt_count(overview.jobs_cancelled) tone="muted" />
                            <StatCard label="Total jobs" value=fmt_count(overview.jobs_total) />
                            <StatCard label="Workers" value=fmt_count(cluster.available_workers as i64) />
                            <StatCard label="Running tasks" value=fmt_count(cluster.running_tasks as i64) tone="running" />
                        </div>
                        <div class="panel">
                            <h2>"Cluster"</h2>
                            <div class="kv-grid">
                                <div class="kv">
                                    <div class="kv-label">"Leader"</div>
                                    <div class="kv-value mono">{cluster.leader_id.clone()}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Workers"</div>
                                    <div class="kv-value">{cluster.available_workers}</div>
                                </div>
                                <div class="kv">
                                    <div class="kv-label">"Tasks total / running"</div>
                                    <div class="kv-value">{format!("{} / {}", cluster.total_tasks, cluster.running_tasks)}</div>
                                </div>
                            </div>
                        </div>
                        <div class="panel">
                            <h2>"Jobs by state"</h2>
                            <table>
                                <thead>
                                    <tr><th>"State"</th><th>"Jobs"</th></tr>
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
                .unwrap_or_else(|| view! { <div class="loading">"Loading…"</div> }.into_any())
        }}
    }
}
