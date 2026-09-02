// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Cluster view: workers and their heartbeats.

use crate::api;
use crate::app::{mark_refreshed, use_polling};
use crate::fmt::fmt_time;
use crate::i18n::{t, tf};
use crate::ui::ErrorBanner;
use leptos::prelude::*;
use leptos::task::spawn_local;

#[component]
pub fn Cluster() -> impl IntoView {
    let (data, set_data) = RwSignal::new_local(None::<api::ClusterInfo>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();

    use_polling(move || {
        spawn_local(async move {
            match api::cluster().await {
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
                .map(|cluster| {
                    let role = if cluster.leader_role.is_empty() {
                        "-".to_string()
                    } else {
                        cluster.leader_role.clone()
                    };
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
                                    <crate::ui::StatCard label=t("cl.workers") value=cluster.available_workers.to_string() />
                                    <crate::ui::StatCard label=t("ov.running_tasks") value=cluster.running_tasks.to_string() tone="running" />
                                    <crate::ui::StatCard label=t("cl.total_tasks") value=cluster.total_tasks.to_string() tone="muted" />
                                    <crate::ui::StatCard label=t("cl.overloaded") value=label tone=tone />
                                </div>
                            }
                        }}
                        <div class="panel">
                            <h2>{t("cl.masters")}</h2>
                            <div class="checkpoint-list">
                                {cluster
                                    .raft_members
                                    .iter()
                                    .map(|member| {
                                        let is_leader = *member == cluster.leader_id
                                            && !cluster.leader_id.is_empty();
                                        view! {
                                            <span class="checkpoint-chip">
                                                <span class="mono">{member.clone()}</span>
                                                {is_leader.then(|| {
                                                    view! {
                                                        <span class="badge ok">{t("cl.leader_badge")}</span>
                                                    }
                                                })}
                                            </span>
                                        }
                                    })
                                    .collect::<Vec<_>>()}
                            </div>
                            {cluster.raft_members.is_empty().then(|| {
                                view! { <div class="muted">"—"</div> }
                            })}
                        </div>
                        <div class="panel">
                            <h2>
                                {tf(
                                    "cl.table_title",
                                    &[&cluster.leader_id.clone(), &cluster.leader_term.to_string(), &role],
                                )}
                            </h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>{t("cl.col.worker_id")}</th>
                                        <th>{t("cl.col.address")}</th>
                                        <th>{t("cl.col.status")}</th>
                                        <th>{t("cl.col.load")}</th>
                                        <th>{t("cl.col.lag")}</th>
                                        <th>{t("cl.col.memory")}</th>
                                        <th>{t("cl.col.cpu")}</th>
                                        <th>{t("cl.col.tasks")}</th>
                                        <th>{t("cl.col.heartbeat")}</th>
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
                                                view! { <span class="badge ok">{t("cl.accepting")}</span> }
                                            } else {
                                                view! { <span class="badge bad">{t("cl.overloaded_bad")}</span> }
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
                                                    <td class="mono">
                                                        {format!("{}%", worker.cpu_permille as f64 / 10.0)}
                                                    </td>
                                                    <td>{worker.running_tasks}</td>
                                                    <td>{fmt_time(worker.last_heartbeat_ms)}</td>
                                                </tr>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tbody>
                            </table>
                            <p class="hint">{t("cl.hint")}</p>
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
