// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Lightweight in-house UI components (badges, cards, modal, banners) plus
//! the toast notification stack and a confirmation dialog.

use leptos::prelude::*;
use leptos::task::spawn_local;
use std::sync::atomic::{AtomicU64, Ordering};

/// Colored state badge for job/task states and health. Transitional states
/// (CREATED/SCHEDULED/DEPLOYING) get the neutral amber tone instead of the
/// green "completed" tone they used to share.
#[component]
pub fn StateTag(#[prop(into)] state: String) -> impl IntoView {
    let class = match state.as_str() {
        "RUNNING" | "ok" => "tag tag-running",
        "COMPLETED" => "tag tag-completed",
        "FAILED" | "degraded" => "tag tag-failed",
        "CANCELLED" => "tag tag-cancelled",
        _ => "tag tag-pending",
    };
    view! { <span class=class>{state}</span> }
}

/// Numeric summary card for the overview page. `tone` (optional) colors the
/// value: "running" (blue), "completed" (green), "failed" (red), "muted".
#[component]
pub fn StatCard(
    #[prop(into)] label: String,
    value: String,
    #[prop(optional, into)] tone: Option<&'static str>,
) -> impl IntoView {
    let class = match tone {
        Some(t) => format!("card tone-{}", t),
        None => "card".to_string(),
    };
    view! {
        <div class=class>
            <div class="value">{value}</div>
            <div class="label">{label}</div>
        </div>
    }
}

/// Red error banner; hidden when the message is `None`.
#[component]
pub fn ErrorBanner(message: Signal<Option<String>>) -> impl IntoView {
    move || {
        match message.get() {
            Some(message) => view! { <div class="error-banner">{message}</div> }.into_any(),
            None => ().into_any(),
        }
    }
}

/// Simple modal dialog closed by clicking the backdrop or the × button.
/// Visibility toggles via a CSS class so the children render only once.
#[component]
pub fn Modal(
    show: RwSignal<bool>,
    #[prop(into)] title: Signal<String>,
    children: Children,
) -> impl IntoView {
    view! {
        <div
            class="modal-backdrop"
            class:hidden=move || !show.get()
            on:click=move |_| show.set(false)
        >
            <div class="modal" on:click=|event| event.stop_propagation()>
                <div class="modal-header">
                    <h3>{move || title.get()}</h3>
                    <button on:click=move |_| show.set(false)>"×"</button>
                </div>
                <div class="modal-body">{children()}</div>
            </div>
        </div>
    }
}

// --- Toasts ------------------------------------------------------------------

/// Kind of a toast notification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToastKind {
    Success,
    Error,
    Info,
}

#[derive(Clone)]
pub(crate) struct Toast {
    pub id: u64,
    pub kind: ToastKind,
    pub text: String,
}

thread_local! {
    static TOASTS: std::cell::OnceCell<RwSignal<Vec<Toast>>> = const { std::cell::OnceCell::new() };
    static NEXT_TOAST_ID: std::cell::OnceCell<AtomicU64> = const { std::cell::OnceCell::new() };
}

const TOAST_TTL_MS: u32 = 4_000;
const MAX_TOASTS: usize = 5;

fn toasts() -> RwSignal<Vec<Toast>> {
    TOASTS.with(|cell| *cell.get_or_init(|| RwSignal::new(Vec::new())))
}

/// Root the toast signal before mount (see `app::init_globals`).
pub fn init_toasts() {
    let _ = toasts();
}

/// Show a transient notification in the bottom-right stack; auto-dismisses.
pub fn push_toast(kind: ToastKind, text: impl Into<String>) {
    let id = NEXT_TOAST_ID.with(|cell| {
        cell.get_or_init(|| AtomicU64::new(1))
            .fetch_add(1, Ordering::Relaxed)
    });
    toasts().update(|items| {
        items.push(Toast {
            id,
            kind,
            text: text.into(),
        });
        // Keep the stack bounded; oldest disappear first.
        let overflow = items.len().saturating_sub(MAX_TOASTS);
        items.drain(..overflow);
    });
    spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(TOAST_TTL_MS).await;
        toasts().update(|items| items.retain(|toast| toast.id != id));
    });
}

/// Fixed-position host rendering the active toast stack. Mounted once by
/// the shell.
#[component]
pub fn ToastHost() -> impl IntoView {
    move || {
        toasts()
            .get()
            .into_iter()
            .map(|toast| {
                let tone = match toast.kind {
                    ToastKind::Success => "success",
                    ToastKind::Error => "error",
                    ToastKind::Info => "info",
                };
                view! { <div class=format!("toast toast-{}", tone)>{toast.text}</div> }
            })
            .collect::<Vec<_>>()
    }
}

// --- Confirmation dialog -----------------------------------------------------

/// Confirmation dialog for destructive actions. The parent owns `show`,
/// and receives the confirmation through `on_confirm` (the dialog closes
/// itself afterwards). Render it inside a reactive closure so its labels
/// follow the language switch.
#[component]
pub fn ConfirmDialog(
    show: RwSignal<bool>,
    #[prop(into)] title: String,
    message: Signal<String>,
    #[prop(into)] confirm_label: String,
    #[prop(optional, into)] danger: bool,
    on_confirm: Callback<()>,
) -> impl IntoView {
    let cancel_label = crate::i18n::t("misc.cancel");
    view! {
        <div class="modal-backdrop" class:hidden=move || !show.get() on:click=move |_| show.set(false)>
            <div class="modal modal-confirm" on:click=|event| event.stop_propagation()>
                <div class="modal-header">
                    <h3>{title}</h3>
                    <button on:click=move |_| show.set(false)>"×"</button>
                </div>
                <div class="modal-body">{move || message.get()}</div>
                <div class="modal-footer">
                    <button on:click=move |_| show.set(false)>{cancel_label}</button>
                    <button
                        class=if danger { "danger" } else { "primary" }
                        on:click=move |_| {
                            on_confirm.run(());
                            show.set(false);
                        }
                    >
                        {confirm_label}
                    </button>
                </div>
            </div>
        </div>
    }
}
