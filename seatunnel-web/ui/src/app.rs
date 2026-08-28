// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Application shell: session gate, sidebar navigation, health indicator
//! and routes.

use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::components::{A, Route, Router, Routes};
use leptos_router::path;

use crate::pages::{cluster::Cluster, job_detail::JobDetail, jobs::Jobs, login::Login, overview::Overview};
use crate::ui::StateTag;

/// Session state shared with every page.
#[derive(Clone, Debug, PartialEq)]
pub enum AuthStatus {
    /// `whoami` probe still in flight.
    Unknown,
    /// No valid session: show the login form.
    LoggedOut,
    /// Authenticated as this user.
    User(String),
}

/// Context handle for the global auth signal.
#[derive(Clone, Copy)]
pub struct AuthState(pub RwSignal<AuthStatus>);

const POLL_INTERVAL_MS: u32 = 5_000;

/// Polling interval used by every page.
pub fn poll_interval() -> u32 {
    POLL_INTERVAL_MS
}

#[component]
pub fn App() -> impl IntoView {
    let auth = RwSignal::new(AuthStatus::Unknown);
    provide_context(AuthState(auth));

    // Probe the session cookie once on startup.
    spawn_local(async move {
        match crate::api::whoami().await {
            Ok(identity) => auth.set(AuthStatus::User(identity.username)),
            Err(_) => auth.set(AuthStatus::LoggedOut),
        }
    });

    view! {
        <Router>
            <Show
                when=move || auth.get() != AuthStatus::Unknown
                fallback=|| view! { <div class="loading">"Loading…"</div> }
            >
                <Show
                    when=move || matches!(auth.get(), AuthStatus::User(_))
                    fallback=move || view! { <Login /> }
                >
                    <Shell auth />
                </Show>
            </Show>
        </Router>
    }
}

/// Authenticated application chrome: sidebar, topbar (identity + logout)
/// and the routed pages.
#[component]
fn Shell(auth: RwSignal<AuthStatus>) -> impl IntoView {
    let auto_refresh = RwSignal::new(true);
    provide_context(RefreshControl(auto_refresh));

    let (health, set_health) = RwSignal::new_local(None::<crate::api::Health>).split();
    spawn_local(async move {
        loop {
            if auto_refresh.get_untracked() {
                set_health.set(crate::api::health().await.ok());
            }
            gloo_timers::future::TimeoutFuture::new(POLL_INTERVAL_MS).await;
        }
    });

    let on_logout = move |_| {
        let auth = auth;
        spawn_local(async move {
            crate::api::logout().await;
            auth.set(AuthStatus::LoggedOut);
        });
    };

    view! {
        // The Router renders an anonymous wrapper div; the shell flex layout
        // keeps sidebar and content side by side inside it.
        <div class="shell">
            <div class="sidebar">
                <div class="brand">"⬡ SeaTunnel"</div>
                <nav>
                    <A href="/">"Overview"</A>
                    <A href="/jobs">"Jobs"</A>
                    <A href="/cluster">"Cluster"</A>
                </nav>
            </div>
            <div class="content">
                <div class="topbar">
                    <h1>"Management Console"</h1>
                    <div class="controls">
                        <label>
                            <input
                                type="checkbox"
                                prop:checked=move || auto_refresh.get()
                                on:change=move |event| {
                                    auto_refresh.set(event_target_checked(&event));
                                }
                            />
                            " auto-refresh (5s)"
                        </label>
                        {move || {
                            health
                                .get()
                                .map(|health| {
                                    view! {
                                        <span class="muted mono">{health.master}</span>
                                        <span title=health.error.clone()>
                                            <StateTag state=health.status />
                                        </span>
                                    }
                                    .into_any()
                                })
                                .unwrap_or_else(|| {
                                    view! { <span class="muted">"connecting…"</span> }.into_any()
                                })
                        }}
                        {move || {
                            match auth.get() {
                                AuthStatus::User(user) => view! {
                                    <span class="muted">"👤 "{user}</span>
                                    <button on:click=on_logout>"Logout"</button>
                                }
                                    .into_any(),
                                _ => ().into_any(),
                            }
                        }}
                    </div>
                </div>
                <Routes fallback=|| {
                    view! { <leptos_router::components::Redirect path="/" /> }.into_view()
                }>
                    <Route path=path!("/") view=Overview />
                    <Route path=path!("/jobs") view=Jobs />
                    <Route path=path!("/jobs/:id") view=JobDetail />
                    <Route path=path!("/cluster") view=Cluster />
                </Routes>
            </div>
        </div>
    }
}

/// Context flag shared with all pages: auto-refresh on/off.
#[derive(Clone, Copy)]
pub struct RefreshControl(pub RwSignal<bool>);
