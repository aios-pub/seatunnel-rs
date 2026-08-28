// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Lightweight in-house UI components (badges, cards, modal, banners).

use leptos::prelude::*;
use leptos::children::Children;

/// Colored state badge for job/task states and health.
#[component]
pub fn StateTag(#[prop(into)] state: String) -> impl IntoView {
    let class = match state.as_str() {
        "RUNNING" | "ok" => "tag tag-running",
        "COMPLETED" | "SCHEDULED" | "CREATED" => "tag tag-completed",
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
    label: &'static str,
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

/// Green success banner; hidden when the message is `None`.
#[component]
pub fn SuccessBanner(message: Signal<Option<String>>) -> impl IntoView {
    move || {
        match message.get() {
            Some(message) => view! { <div class="success-banner">{message}</div> }.into_any(),
            None => ().into_any(),
        }
    }
}

/// Simple modal dialog closed by clicking the backdrop or the × button.
/// Visibility toggles via a CSS class so the children render only once.
#[component]
pub fn Modal(show: RwSignal<bool>, title: &'static str, children: Children) -> impl IntoView {
    view! {
        <div
            class="modal-backdrop"
            class:hidden=move || !show.get()
            on:click=move |_| show.set(false)
        >
            <div class="modal" on:click=|event| event.stop_propagation()>
                <div class="modal-header">
                    <h3>{title}</h3>
                    <button on:click=move |_| show.set(false)>"×"</button>
                </div>
                <div class="modal-body">{children()}</div>
            </div>
        </div>
    }
}
