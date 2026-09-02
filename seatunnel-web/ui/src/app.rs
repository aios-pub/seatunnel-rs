// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Application shell: session gate, sidebar navigation, health indicator
//! and routes. Also hosts the console-wide reactive singletons (refresh
//! controls, last-update stamp) and the shared polling helper.

use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::components::{A, Route, Router, Routes};
use leptos_router::path;

use crate::i18n;
use crate::i18n::Lang;
use crate::pages::{
    cluster::Cluster, job_detail::JobDetail, jobs::Jobs, login::Login, logs::Logs, overview::Overview,
    worker::WorkerDetail,
};
use crate::ui::{push_toast, StateTag, ToastHost, ToastKind};

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

// --- Console-wide singletons -------------------------------------------------
// Root-owned signals created once per page load and reachable from anywhere,
// so the login page and the shell share state without context plumbing.

thread_local! {
    static AUTO_REFRESH: std::cell::OnceCell<RwSignal<bool>> = const { std::cell::OnceCell::new() };
    static REFRESH_BUMP: std::cell::OnceCell<RwSignal<u64>> = const { std::cell::OnceCell::new() };
    static LAST_UPDATE: std::cell::OnceCell<RwSignal<Option<f64>>> = const { std::cell::OnceCell::new() };
    static THEME: std::cell::OnceCell<RwSignal<Theme>> = const { std::cell::OnceCell::new() };
}

/// UI color scheme.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Theme {
    #[default]
    Light,
    Dark,
}

impl Theme {
    fn key(self) -> &'static str {
        match self {
            Theme::Light => "light",
            Theme::Dark => "dark",
        }
    }

    fn from_key(key: &str) -> Self {
        match key {
            "dark" => Theme::Dark,
            _ => Theme::Light,
        }
    }

    fn other(self) -> Self {
        match self {
            Theme::Light => Theme::Dark,
            Theme::Dark => Theme::Light,
        }
    }

    /// Icon shown on the toggle (the theme you would switch TO).
    fn icon(self) -> &'static str {
        match self {
            Theme::Light => "🌙",
            Theme::Dark => "☀️",
        }
    }
}

/// Global theme signal (localStorage-persisted).
pub fn theme() -> RwSignal<Theme> {
    THEME.with(|cell| {
        *cell.get_or_init(|| {
            let stored = window()
                .local_storage()
                .ok()
                .flatten()
                .and_then(|storage| storage.get_item("seatunnel_theme").ok())
                .flatten()
                .map(|key| Theme::from_key(&key));
            RwSignal::new(stored.unwrap_or_default())
        })
    })
}

/// Switch the UI color scheme and remember the choice.
pub fn set_theme(next: Theme) {
    theme().set(next);
    if let Some(storage) = window().local_storage().ok().flatten() {
        let _ = storage.set_item("seatunnel_theme", next.key());
    }
}

/// Create every console-wide root signal BEFORE the app mounts. Signals
/// created lazily during a render end up owned by that render's reactive
/// scope and are disposed with it (the Show fallback scope is torn down on
/// the first transition), so they must be rooted here instead.
pub fn init_globals() {
    let _ = auto_refresh();
    let _ = refresh_bump();
    let _ = last_update();
    let _ = i18n::lang();
    let _ = theme();
    crate::ui::init_toasts();
}

/// Global auto-refresh toggle (bound to the topbar checkbox).
pub fn auto_refresh() -> RwSignal<bool> {
    AUTO_REFRESH.with(|cell| *cell.get_or_init(|| RwSignal::new(true)))
}

/// Monotonic counter bumped by "Refresh now".
pub fn refresh_bump() -> RwSignal<u64> {
    REFRESH_BUMP.with(|cell| *cell.get_or_init(|| RwSignal::new(0)))
}

/// Epoch-ms of the last successful data refresh, for the topbar stamp.
pub fn last_update() -> RwSignal<Option<f64>> {
    LAST_UPDATE.with(|cell| *cell.get_or_init(|| RwSignal::new(None)))
}

/// Request an immediate refresh of every mounted page.
pub fn request_refresh() {
    refresh_bump().update(|value| *value += 1);
}

/// Record a successful data refresh (called by page fetch closures).
pub fn mark_refreshed() {
    last_update().set(Some(js_sys::Date::now()));
}

/// Poll-loop helper with the lifecycle the pages used to hand-roll: fetches
/// once on mount and again whenever auto-refresh is on or the user pressed
/// "Refresh now"; pauses while the tab is hidden; stops when the component
/// unmounts (the old ad-hoc loops leaked one loop per page visit).
pub fn use_polling(fetch: impl Fn() + Clone + 'static) {
    let auto = auto_refresh();
    let bump = refresh_bump();

    // Immediate fetch on mount plus on every "Refresh now".
    Effect::new({
        let fetch = fetch.clone();
        move || {
            let _tick = bump.get();
            fetch();
        }
    });

    let (dead, set_dead) = RwSignal::new_local(false).split();
    spawn_local(async move {
        let mut seen_bump = bump.get_untracked();
        loop {
            gloo_timers::future::TimeoutFuture::new(POLL_INTERVAL_MS).await;
            if dead.get_untracked() {
                return;
            }
            let hidden = window()
                .document()
                .map(|doc| doc.visibility_state() == web_sys::VisibilityState::Hidden)
                .unwrap_or(false);
            if hidden {
                continue;
            }
            let tick = bump.get_untracked();
            let manual = tick != seen_bump;
            if manual {
                seen_bump = tick;
            }
            if manual || auto.get_untracked() {
                fetch();
            }
        }
    });
    on_cleanup(move || set_dead.set(true));
}

#[component]
pub fn App() -> impl IntoView {
    let auth = RwSignal::new(AuthStatus::Unknown);
    provide_context(AuthState(auth));

    // Reflect the theme signal onto <html data-theme="..."> so the CSS
    // variable overrides apply.
    Effect::new(move || {
        let key = theme().get().key();
        if let Some(element) = document().document_element() {
            let _ = element.set_attribute("data-theme", key);
        }
    });

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
                fallback=|| view! { <div class="loading">{i18n::t("misc.loading")}</div> }
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

/// Authenticated application chrome: sidebar, topbar (refresh controls,
/// health, language, identity) and the routed pages.
#[component]
fn Shell(auth: RwSignal<AuthStatus>) -> impl IntoView {
    let (health, set_health) = RwSignal::new_local(None::<crate::api::Health>).split();
    // Toast "master unreachable" once per degradation, not once per poll.
    let (degraded, set_degraded) = RwSignal::new_local(false).split();

    use_polling(move || {
        spawn_local(async move {
            match crate::api::health().await {
                Ok(value) => {
                    let is_degraded = value.status != "ok";
                    if is_degraded && !degraded.get_untracked() {
                        push_toast(ToastKind::Error, i18n::t("topbar.health_degraded"));
                    }
                    set_degraded.set(is_degraded);
                    set_health.set(Some(value));
                }
                Err(_) => set_health.set(None),
            }
        })
    });

    let on_logout = move |_| {
        let auth = auth;
        spawn_local(async move {
            crate::api::logout().await;
            auth.set(AuthStatus::LoggedOut);
        });
    };

    let on_toggle_lang = move |_| i18n::toggle_lang();

    view! {
        // The Router renders an anonymous wrapper div; the shell flex layout
        // keeps sidebar and content side by side inside it.
        <div class="shell">
            <div class="sidebar">
                <div class="brand">"⬡ SeaTunnel"</div>
                <nav>
                    <A href="/">{move || i18n::t("nav.overview")}</A>
                    <A href="/jobs">{move || i18n::t("nav.jobs")}</A>
                    <A href="/cluster">{move || i18n::t("nav.cluster")}</A>
                    <A href="/logs">{move || i18n::t("nav.logs")}</A>
                </nav>
            </div>
            <div class="content">
                <div class="topbar">
                    <h1>{move || i18n::t("topbar.title")}</h1>
                    <div class="controls">
                        <button title=move || i18n::t("topbar.refresh_now") on:click=move |_| request_refresh()>
                            "⟳"
                        </button>
                        <label>
                            <input
                                type="checkbox"
                                prop:checked=move || auto_refresh().get()
                                on:change=move |event| {
                                    auto_refresh().set(event_target_checked(&event));
                                }
                            />
                            {move || i18n::t("topbar.auto_refresh")}
                        </label>
                        {move || {
                            last_update()
                                .get()
                                .map(|ms| i18n::tf("topbar.updated", &[&crate::fmt::fmt_time(ms as i64)]))
                        }}
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
                                    view! { <span class="muted">{i18n::t("topbar.connecting")}</span> }
                                        .into_any()
                                })
                        }}
                        <button
                            title=move || i18n::t("topbar.theme")
                            on:click=move |_| {
                                let next = theme().get_untracked().other();
                                set_theme(next);
                            }
                        >
                            {move || theme().get().icon()}
                        </button>
                        <button on:click=on_toggle_lang>
                            {move || i18n::lang().get().toggle_label()}
                        </button>
                        {move || {
                            match auth.get() {
                                AuthStatus::User(user) => view! {
                                    <span class="muted">"👤 "{user}</span>
                                    <button on:click=on_logout>{move || i18n::t("topbar.logout")}</button>
                                }
                                    .into_any(),
                                _ => ().into_any(),
                            }
                        }}
                    </div>
                </div>
                <Routes fallback=|| {
                    view! { <NotFound /> }.into_view()
                }>
                    <Route path=path!("/") view=Overview />
                    <Route path=path!("/jobs") view=Jobs />
                    <Route path=path!("/jobs/:id") view=JobDetail />
                    <Route path=path!("/cluster") view=Cluster />
                    <Route path=path!("/cluster/workers/:id") view=WorkerDetail />
                    <Route path=path!("/logs") view=Logs />
                    <Route path=path!("/*any") view=NotFound />
                </Routes>
            </div>
        </div>
        <ToastHost />
    }
}

/// Shown for any URL no route matches (replaces the old silent redirect).
#[component]
fn NotFound() -> impl IntoView {
    view! {
        <div class="panel">
            <h2>{move || i18n::t("nf.title")}</h2>
            <a href="/">{move || i18n::t("nf.back")}</a>
        </div>
    }
}

/// Language currently selected — re-exported so pages can match on it.
#[allow(unused)]
fn _lang_type_use(_: Lang) {}
