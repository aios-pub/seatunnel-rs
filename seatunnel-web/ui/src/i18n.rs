// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Minimal bilingual (EN / 中文) UI dictionary backed by a global language
//! signal. `t()` / `tf()` read the signal reactively: calling them inside a
//! reactive closure makes the closure re-run on a language switch.

use leptos::prelude::*;
use std::cell::OnceCell;

/// UI language.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Lang {
    #[default]
    En,
    Zh,
}

impl Lang {
    fn key(self) -> &'static str {
        match self {
            Lang::En => "en",
            Lang::Zh => "zh",
        }
    }

    fn from_key(key: &str) -> Self {
        match key {
            "zh" => Lang::Zh,
            _ => Lang::En,
        }
    }

    /// Label of the language a toggle button would switch TO.
    pub fn toggle_label(self) -> &'static str {
        match self {
            Lang::En => "中文",
            Lang::Zh => "EN",
        }
    }

    fn other(self) -> Self {
        match self {
            Lang::En => Lang::Zh,
            Lang::Zh => Lang::En,
        }
    }
}

thread_local! {
    static LANG: OnceCell<RwSignal<Lang>> = OnceCell::new();
}

/// Global language signal, initialized from localStorage with the browser
/// language as fallback. Created once per page load.
pub fn lang() -> RwSignal<Lang> {
    LANG.with(|cell| {
        *cell.get_or_init(|| {
            let stored = window()
                .local_storage()
                .ok()
                .flatten()
                .and_then(|storage| storage.get_item("seatunnel_lang").ok())
                .flatten()
                .map(|key| Lang::from_key(&key));
            let fallback = || {
                let nav = window().navigator().language().unwrap_or_default();
                if nav.starts_with("zh") {
                    Lang::Zh
                } else {
                    Lang::En
                }
            };
            RwSignal::new(stored.unwrap_or_else(fallback))
        })
    })
}

/// Switch the UI language and remember the choice.
pub fn set_lang(next: Lang) {
    lang().set(next);
    if let Some(storage) = window().local_storage().ok().flatten() {
        let _ = storage.set_item("seatunnel_lang", next.key());
    }
}

/// Toggle between English and Chinese.
pub fn toggle_lang() {
    set_lang(lang().get_untracked().other());
}

/// Translate `key` in the current language. Missing keys fall back to
/// English, then to the key itself (which makes omissions obvious). Reads
/// the language signal tracked, so views calling `t()` inside reactive
/// closures re-render on a language switch.
pub fn t(key: &str) -> String {
    let selected = lang().get();
    let entry = DICT
        .iter()
        .find(|(k, _, _)| *k == key)
        .map(|(_, en, zh)| (*en, *zh));
    match entry {
        Some((en, zh)) => match selected {
            Lang::En => en.to_string(),
            Lang::Zh => zh.to_string(),
        },
        None => key.to_string(),
    }
}

/// Translate `key`, substituting each `{}` placeholder with the next entry
/// of `args` in order.
pub fn tf(key: &str, args: &[&str]) -> String {
    let mut out = t(key);
    for arg in args {
        match out.find("{}") {
            Some(pos) => out.replace_range(pos..pos + 2, arg),
            None => break,
        }
    }
    out
}

/// `(key, english, chinese)` dictionary; linear scan is fine at this size.
#[rustfmt::skip]
const DICT: &[(&str, &str, &str)] = &[
    // Navigation & shell
    ("nav.overview", "Overview", "总览"),
    ("nav.jobs", "Jobs", "作业"),
    ("nav.cluster", "Cluster", "集群"),
    ("topbar.title", "Management Console", "管理控制台"),
    ("topbar.auto_refresh", "Auto refresh (5s)", "自动刷新 (5s)"),
    ("topbar.refresh_now", "Refresh now", "立即刷新"),
    ("topbar.updated", "Updated {}", "更新于 {}"),
    ("topbar.connecting", "connecting…", "连接中…"),
    ("topbar.logout", "Logout", "退出"),
    ("topbar.health_degraded", "Master unreachable — console data may be stale.", "Master 不可达，控制台数据可能过期。"),
    // Login
    ("login.hint", "Sign in to the management console", "登录管理控制台"),
    ("login.username", "Username", "用户名"),
    ("login.password", "Password", "密码"),
    ("login.sign_in", "Sign in", "登录"),
    ("login.signing_in", "Signing in…", "登录中…"),
    // Overview
    ("ov.running_jobs", "Running jobs", "运行中作业"),
    ("ov.pending", "Pending", "排队中"),
    ("ov.completed", "Completed", "已完成"),
    ("ov.failed", "Failed", "已失败"),
    ("ov.cancelled", "Cancelled", "已取消"),
    ("ov.total_jobs", "Total jobs", "作业总数"),
    ("ov.workers", "Workers", "工作节点"),
    ("ov.running_tasks", "Running tasks", "运行中任务"),
    ("ov.cluster", "Cluster", "集群"),
    ("ov.leader", "Leader", "Leader"),
    ("ov.tasks_total_running", "Tasks total / running", "任务总数 / 运行中"),
    ("ov.jobs_by_state", "Jobs by state", "按状态统计"),
    ("ov.state", "State", "状态"),
    ("ov.jobs", "Jobs", "作业数"),
    // Jobs page
    ("jobs.submit", "Submit job", "提交作业"),
    ("jobs.count", "{} jobs", "{} 个作业"),
    ("jobs.col.job", "Job", "作业名"),
    ("jobs.col.job_id", "Job ID", "作业 ID"),
    ("jobs.col.state", "State", "状态"),
    ("jobs.col.started", "Started", "开始时间"),
    ("jobs.col.duration", "Duration", "运行时长"),
    ("jobs.col.actions", "Actions", "操作"),
    ("jobs.cancel", "Stop", "停止"),
    ("jobs.cancel_confirm", "Stop job {}? It stops after a final checkpoint (savepoint semantics).", "停止作业 {}？将先触发最终 checkpoint（保存点语义）再停止。"),
    ("jobs.cancel_failed", "Stop failed", "停止失败"),
    ("jobs.cancel_title", "Stop job {}", "停止作业 {}"),
    ("jobs.delete", "Delete", "删除"),
    ("jobs.delete_title", "Delete job {}", "删除作业 {}"),
    ("jobs.delete_confirm", "Delete job {} from history? Its state and checkpoint metadata are removed and this cannot be undone.", "从历史中删除作业 {}？其状态与 checkpoint 元数据将被移除，且不可恢复。"),
    ("jobs.delete_failed", "Delete failed", "删除失败"),
    ("jobs.deleted", "Job {} deleted", "作业 {} 已删除"),
    ("jobs.filter_all", "All states", "全部状态"),
    ("jobs.search_ph", "Search name or ID…", "搜索名称或 ID…"),
    ("jobs.sort.newest", "Newest first", "最新优先"),
    ("jobs.sort.oldest", "Oldest first", "最早优先"),
    ("jobs.sort.name", "By name", "按名称"),
    ("jobs.sort.duration", "By duration", "按时长"),
    ("jobs.batch_stop", "Stop selected ({})", "停止所选 ({})"),
    ("jobs.batch_stop_confirm", "Stop {} running job(s)? Each stops after a final checkpoint.", "停止 {} 个运行中的作业？每个作业都会先触发最终 checkpoint。"),
    ("jobs.select_all", "Select all", "全选"),
    ("jobs.batch_stopped", "Stopped {} job(s)", "已停止 {} 个作业"),
    ("jobs.file", "Load from file", "从文件载入"),
    ("jobs.invalid_json", "Invalid JSON config.", "JSON 配置解析失败。"),
    ("jobs.page_info", "{}–{} of {}", "第 {}–{} 项，共 {} 项"),
    ("jobs.prev", "← Prev", "← 上一页"),
    ("jobs.next", "Next →", "下一页 →"),
    ("jobs.dialog_title", "Submit job", "提交作业"),
    ("jobs.config", "Job config", "作业配置"),
    ("jobs.format", "Format", "格式"),
    ("jobs.name_opt", "Job name (optional)", "作业名（可选）"),
    ("jobs.parallelism_opt", "Parallelism (optional)", "并行度（可选）"),
    ("jobs.parallelism_invalid", "Parallelism must be a positive integer.", "并行度必须为正整数。"),
    ("jobs.close", "Close", "关闭"),
    ("jobs.submit_btn", "Submit", "提交"),
    ("jobs.submitting", "Submitting…", "提交中…"),
    ("jobs.submitted", "Job {} submitted", "作业 {} 已提交"),
    ("jobs.submit_failed", "Submit failed", "提交失败"),
    // Job detail
    ("jd.job_id", "Job ID", "作业 ID"),
    ("jd.started", "Started", "开始时间"),
    ("jd.duration", "Duration", "运行时长"),
    ("jd.cp_interval", "Checkpoint interval", "Checkpoint 间隔"),
    ("jd.cp_completed", "Checkpoints completed", "已完成 checkpoint"),
    ("jd.tasks", "Tasks", "任务"),
    ("jd.parallelism", "Parallelism", "并行度"),
    ("jd.metrics", "Metrics", "指标"),
    ("jd.edit_restart", "Edit & restart", "编辑配置并重启"),
    ("jd.resubmit", "Resubmit same id", "以同 ID 重新提交"),
    ("jd.restart", "Restart", "重启"),
    ("jd.updating", "Updating…", "更新中…"),
    ("jd.restarting", "Restarting…", "重启中…"),
    ("jd.confirm_update", "Confirm update & restart", "确认更新并重启"),
    ("jd.editor_title", "Edit job configuration (JSON)", "编辑作业配置 (JSON)"),
    ("jd.restart_confirm", "Restart job {}? The running incarnation is cancelled with a final checkpoint, then resubmitted with the same id (tasks restore from the latest checkpoint).", "重启作业 {}？当前实例将触发最终 checkpoint 后停止，并以相同 ID 重新提交（任务从最新 checkpoint 恢复）。"),
    ("jd.edit_restart_confirm", "Stop the current incarnation with a final checkpoint and restart it with the edited config (same job id)?", "以最终 checkpoint 停止当前实例，并使用编辑后的配置重启（作业 ID 不变）？"),
    ("jd.update_running", "Stopping the old incarnation (final checkpoint) and resubmitting…", "正在停止旧实例（最终 checkpoint）并重新提交…"),
    ("jd.updated", "Updated: {} (cancel took {} ms); the job restores from its latest checkpoint.", "已更新：{}（取消耗时 {} ms）；作业将从最新 checkpoint 恢复。"),
    ("jd.update_failed", "Update failed: {}", "更新失败：{}"),
    ("jd.restart_started", "Restarting with the retained config (cancelling the old incarnation first)…", "正在使用保留配置重启（先取消旧实例）…"),
    ("jd.restarted", "Restarted: {}", "已重启：{}"),
    ("jd.restart_failed", "Restart failed: {}", "重启失败：{}"),
    ("jd.hint", "Update = stop (automatic exit checkpoint) then resubmit with the SAME job id: workers resume from the latest checkpoint (at-least-once; exactly-once with transactional sinks). Cross-worker restore requires s3/master checkpoint storage. Restart = same flow with the ORIGINAL config, no editing.", "更新 = 停止（自动 exit checkpoint）后以相同作业 ID 重新提交：worker 从最新 checkpoint 恢复（at-least-once；事务型 sink 为 exactly-once）。跨节点恢复需要 s3/master checkpoint 存储。重启 = 同样流程但使用原始配置，不做修改。"),
    ("jd.col.task_id", "Task ID", "任务 ID"),
    ("jd.col.worker", "Worker", "工作节点"),
    ("jd.col.processed", "Processed", "已处理"),
    ("jd.col.throughput", "Throughput", "吞吐"),
    ("jd.col.idle", "Idle", "空闲"),
    ("jd.col.sink", "Sink delivery", "Sink 投递"),
    ("jd.col.error", "Error", "错误"),
    ("jd.live_logs", "Live logs", "实时日志"),
    ("jd.lines_title", "{} ({} lines)", "{}（{} 行）"),
    ("jd.autoscroll", "auto-scroll", "自动滚动"),
    ("jd.no_logs", "No task logs yet.", "暂无任务日志。"),
    ("jd.cp_history", "Checkpoint history — {} completed, every {} ms", "Checkpoint 历史 — 已完成 {} 个，间隔 {} ms"),
    ("jd.no_checkpoints", "No checkpoints recorded yet (waiting for the first interval to complete).", "暂无 checkpoint 记录（等待第一个周期完成）。"),
    // Cluster
    ("cl.workers", "Workers", "工作节点"),
    ("cl.total_tasks", "Total tasks", "任务总数"),
    ("cl.overloaded", "Overloaded workers", "过载节点"),
    ("cl.table_title", "Workers (leader: {}, term {}, role {})", "工作节点（Leader: {}，term {}，role {}）"),
    ("cl.col.worker_id", "Worker ID", "节点 ID"),
    ("cl.col.address", "Address", "地址"),
    ("cl.col.status", "Status", "状态"),
    ("cl.col.load", "Load", "负载"),
    ("cl.col.lag", "Lag (ms)", "事件循环延迟 (ms)"),
    ("cl.col.memory", "Memory", "内存"),
    ("cl.col.cpu", "CPU", "CPU"),
    ("cl.col.tasks", "Running tasks", "运行中任务"),
    ("cl.col.heartbeat", "Last heartbeat", "最近心跳"),
    ("cl.accepting", "accepting", "可接收"),
    ("cl.overloaded_bad", "OVERLOADED", "已过载"),
    ("cl.masters", "Masters (raft members)", "Master 节点（raft 成员）"),
    ("cl.history", "History", "历史趋势"),
    ("cl.leader_badge", "leader", "主"),
    ("cl.hint", "Admission is dynamic (no slot counts): a worker accepts new tasks while its event-loop lag stays under the threshold and its memory under the watermark. Overloaded workers stop receiving tasks and their pending tasks are stolen by healthy peers.", "准入是动态的（无固定 slot 数）：事件循环延迟低于阈值且内存低于水位线时，节点才接收新任务。过载节点停止接收任务，其排队任务会被健康节点抢占。"),
    // Not found
    ("nf.title", "404 — page not found", "404 — 页面不存在"),
    ("nf.back", "Back to overview", "返回总览"),
    // Misc
    ("misc.loading", "Loading…", "加载中…"),
    ("misc.no_data", "No data (see the error above).", "暂无数据（见上方错误）。"),
    ("misc.cancel", "Cancel", "取消"),
    // Charts
    ("chart.throughput", "Throughput (rec/s)", "吞吐 (rec/s)"),
    ("chart.sink_latency", "Sink latency EMA (ms)", "Sink 延迟 EMA (ms)"),
    ("chart.worker_load", "Worker load (%)", "节点负载 (%)"),
    ("chart.worker_mem", "Worker memory (%)", "节点内存 (%)"),
    ("chart.worker_cpu", "Worker CPU (%)", "节点 CPU (%)"),
    ("chart.collecting", "Collecting samples…", "正在采集数据…"),
];
