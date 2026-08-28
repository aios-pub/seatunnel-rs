// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Cluster view: workers and their heartbeats.

use crate::api;
use crate::app::{poll_interval, RefreshControl};
use crate::fmt::fmt_time;
use crate::ui::ErrorBanner;
use leptos::prelude::*;
use leptos::task::spawn_local;

#[component]
pub fn Cluster() -> impl IntoView {
    let (data, set_data) = RwSignal::new_local(None::<api::ClusterInfo>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let refresh = expect_context::<RefreshControl>();

    spawn_local(async move {
        loop {
            if refresh.0.get_untracked() {
                match api::cluster().await {
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
                .map(|cluster| {
                    view! {
                        {move || {
                            let overloaded = data
                                .get()
                                .map(|c: crate::api::ClusterInfo| {
                                    c.workers.iter().filter(|w| !w.can_accept).count()
                                })
                                .unwrap_or(0);
                            let (label, tone) = if overloaded > 0 {
                                (format!("{}", overloaded), "failed")
                            } else {
                                ("0".to_string(), "muted")
                            };
                            view! {
                                <div class="cards">
                                    <crate::ui::StatCard label="Workers" value=cluster.available_workers.to_string() />
                                    <crate::ui::StatCard label="Running tasks" value=cluster.running_tasks.to_string() tone="running" />
                                    <crate::ui::StatCard label="Total tasks" value=cluster.total_tasks.to_string() tone="muted" />
                                    <crate::ui::StatCard label="Overloaded workers" value=label tone=tone />
                                </div>
                            }
                        }}
                        <div class="panel">
                            <h2>
                                "Workers (leader: "
                                {cluster.leader_id.clone()}
                                ", term "
                                {cluster.leader_term.to_string()}
                                ", role "
                                {if cluster.leader_role.is_empty() { "-" } else { &cluster.leader_role }}
                                ")"
                            </h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>"Worker ID"</th>
                                        <th>"Address"</th>
                                        <th>"Status"</th>
                                        <th>"Load"</th>
                                        <th>"Lag (ms)"</th>
                                        <th>"Memory"</th>
                                        <th>"Running tasks"</th>
                                        <th>"Last heartbeat"</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {cluster
                                        .workers
                                        .iter()
                                        .map(|worker| {
                                            let load_pct =
                                                (worker.load_score_permille as f64 / 10.0).round() as i32;
                                            let mem_pct =
                                                (worker.mem_permille as f64 / 10.0).round() as i32;
                                            let bar_tone = if !worker.can_accept {
                                                "failed"
                                            } else if load_pct >= 70 {
                                                "running"
                                            } else {
                                                "muted"
                                            };
                                            view! {
                                                <tr class=if worker.can_accept { "" } else { "row-overloaded" }>
                                                    <td class="mono">{worker.worker_id.clone()}</td>
                                                    <td class="mono">{worker.address.clone()}</td>
                                                    <td>
                                        {
                                            if worker.can_accept {
                                                view! { <span class="badge ok">"accepting"</span> }
                                            } else {
                                                view! { <span class="badge bad">"OVERLOADED"</span> }
                                            }.into_any()
                                        }
                                                    </td>
                                                    <td>
                                                        <div class="loadbar">
                                                            <div class=format!("loadbar-fill tone-{}", bar_tone)
                                                                style=format!("width: {}%", load_pct.clamp(0, 100))>
                                                            </div>
                                                        </div>
                                                        <span class="mono small">{format!("{}%", load_pct)}</span>
                                                    </td>
                                                    <td class="mono">{worker.lag_ms}</td>
                                                    <td class="mono">{format!("{}%", mem_pct)}</td>
                                                    <td>{worker.running_tasks}</td>
                                                    <td>{fmt_time(worker.last_heartbeat_ms)}</td>
                                                </tr>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tbody>
                            </table>
                            <p class="hint">
                                "Admission is dynamic (no slot counts): a worker accepts new tasks while its event-loop lag stays under the threshold and its memory under the watermark. Overloaded workers stop receiving tasks and their pending tasks are stolen by healthy peers."
                            </p>
                        </div>
                    }
                    .into_any()
                })
                .unwrap_or_else(|| view! { <div class="loading">"Loading…"</div> }.into_any())
        }}
    }
}
