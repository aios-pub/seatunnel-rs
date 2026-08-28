// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Full-screen login form shown while no session is active.

use crate::api;
use crate::app::AuthState;
use crate::ui::ErrorBanner;
use leptos::prelude::*;
use leptos::task::spawn_local;
use leptos_router::hooks::use_navigate;

#[component]
pub fn Login() -> impl IntoView {
    let username = RwSignal::new(String::new());
    let password = RwSignal::new(String::new());
    let busy = RwSignal::new(false);
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let auth = expect_context::<AuthState>();
    let navigate = use_navigate();

    let on_submit = move |event: leptos::ev::SubmitEvent| {
        event.prevent_default();
        if busy.get_untracked() {
            return;
        }
        let user = username.get_untracked();
        let pass = password.get_untracked();
        let navigate = navigate.clone();
        busy.set(true);
        set_error.set(None);
        spawn_local(async move {
            match api::login(user, pass).await {
                Ok(identity) => {
                    auth.0.set(crate::app::AuthStatus::User(identity.username));
                    navigate("/", Default::default());
                }
                Err(err) => set_error.set(Some(err)),
            }
            busy.set(false);
        });
    };

    view! {
        <div class="login-wrap">
            <form class="login-card" on:submit=on_submit>
                <div class="brand">"⬡ SeaTunnel"</div>
                <div class="hint">"Sign in to the management console"</div>
                <div class="field">
                    <label>"Username"</label>
                    <input
                        type="text"
                        autocomplete="username"
                        prop:value=move || username.get()
                        on:input=move |event| username.set(event_target_value(&event))
                    />
                </div>
                <div class="field">
                    <label>"Password"</label>
                    <input
                        type="password"
                        autocomplete="current-password"
                        prop:value=move || password.get()
                        on:input=move |event| password.set(event_target_value(&event))
                    />
                </div>
                <ErrorBanner message=Signal::derive(move || error.get()) />
                <button class="primary" type="submit" disabled=move || busy.get()>
                    {move || if busy.get() { "Signing in…".to_string() } else { "Sign in".to_string() }}
                </button>
            </form>
        </div>
    }
}
