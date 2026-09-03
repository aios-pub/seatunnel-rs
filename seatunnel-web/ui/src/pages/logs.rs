// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Node log viewer: live-tails the engine's daily rolling log files over
//! SSE with level/substring filters, a download link and fullscreen.

use crate::api;
use crate::i18n::{t, tf};
use crate::ui::ErrorBanner;
use leptos::prelude::*;

#[component]
pub fn Logs() -> impl IntoView {
    let (files, set_files) = RwSignal::new_local(Vec::<String>::new()).split();
    let (no_dir, set_no_dir) = RwSignal::new_local(false).split();
    let (selected, set_selected) = RwSignal::new_local(String::new()).split();
    let (level, set_level) = RwSignal::new_local(String::new()).split();
    let (query, set_query) = RwSignal::new_local(String::new()).split();
    let (lines, set_lines) = RwSignal::new_local(Vec::<String>::new()).split();
    let (stream_error, set_stream_error) = RwSignal::new_local(None::<String>).split();
    let fs = RwSignal::new_local(false);

    crate::log_view::use_escape_on(move || fs.get(), move || fs.set(false));

    // File list: fetched once plus on every "Refresh now".
    {
        let fetch_files = move || {
            leptos::task::spawn_local(async move {
                match api::log_files().await {
                    Ok(value) => {
                        set_no_dir.set(value.error.is_some());
                        set_files.set(value.files);
                    }
                    Err(_) => set_files.set(Vec::new()),
                }
            });
        };
        fetch_files();
        let bump = crate::app::refresh_bump();
        Effect::new(move || {
            let _tick = bump.get();
            fetch_files();
        });
    }

    // Live tail stream, reopened whenever the file/level/filter changes
    // (closing the previous handle drops the old connection). The search
    // box commits on change (Enter/blur) to avoid reopening on every
    // keystroke.
    {
        let handle_cell = std::sync::Arc::new(std::sync::Mutex::new(
            None::<api::StreamHandle>,
        ));
        let handle_for_cleanup = handle_cell.clone();
        Effect::new(move || {
            let name = selected.get();
            let level = level.get();
            let filter = query.get();
            set_lines.set(Vec::new());
            set_stream_error.set(None);
            // Drop the previous stream before opening the new one.
            if let Some(old) = handle_cell.lock().ok().and_then(|mut cell| cell.take()) {
                drop(old);
            }
            if name.is_empty() {
                return;
            }
            let set_lines = set_lines.clone();
            let set_stream_error = set_stream_error.clone();
            match api::stream_log_file(&name, &level, &filter, move |event| {
                if let Some(err) = event.error {
                    set_stream_error.set(Some(err));
                    return;
                }
                set_stream_error.set(None);
                set_lines.update(|current| {
                    if event.reset {
                        *current = event.lines;
                    } else {
                        current.extend(event.lines);
                        let excess = current.len().saturating_sub(5000);
                        if excess > 0 {
                            current.drain(..excess);
                        }
                    }
                });
            }) {
                Ok(handle) => {
                *handle_cell.lock().unwrap() = Some(handle);
            }
                Err(err) => set_stream_error.set(Some(err)),
            }
        });
        on_cleanup(move || {
            if let Some(handle) = handle_for_cleanup.lock().ok().and_then(|mut cell| cell.take()) {
                drop(handle);
            }
        });
    }

    view! {
        <ErrorBanner message=Signal::derive(move || stream_error.get()) />
        <div class="panel" class:logs-fs=move || fs.get()>
            <div class="log-head">
                <h2>{move || {
                    tf("jd.lines_title", &[&t("logs.title"), &lines.get().len().to_string()])
                }}</h2>
                <div class="log-actions">
                    <Show when=move || !selected.get().is_empty()>
                        <a
                            class="btn"
                            href=move || {
                                api::log_file_download_url(
                                    &selected.get(),
                                    5000,
                                    &level.get(),
                                    &query.get(),
                                )
                            }
                        >
                            {t("logs.download")}
                        </a>
                    </Show>
                    <button class="btn" on:click=move |_| fs.update(|value| *value = !*value)>
                        {move || if fs.get() { t("logs.exit_fs") } else { t("logs.fullscreen") }}
                    </button>
                </div>
            </div>
            <div class="toolbar">
                <div class="field">
                    <label>{t("logs.select")}</label>
                    <select
                        class="inline-select"
                        on:change=move |event| set_selected.set(event_target_value(&event))
                    >
                        <option value="">{t("logs.pick")}</option>
                        {move || {
                            files
                                .get()
                                .iter()
                                .rev()
                                .map(|name| {
                                    let value = name.clone();
                                    view! { <option value=value>{name.clone()}</option> }
                                })
                                .collect::<Vec<_>>()
                        }}
                    </select>
                </div>
                <div class="field">
                    <label>{t("logs.level")}</label>
                    <select
                        class="inline-select"
                        on:change=move |event| set_level.set(event_target_value(&event))
                    >
                        <option value="">{t("logs.all")}</option>
                        <option value="ERROR">"ERROR"</option>
                        <option value="WARN">"WARN"</option>
                        <option value="INFO">"INFO"</option>
                        <option value="DEBUG">"DEBUG"</option>
                    </select>
                </div>
                <div class="field">
                    <label>{t("logs.search_ph")}</label>
                    <input
                        class="inline-search"
                        type="search"
                        prop:value=move || query.get()
                        on:change=move |event| set_query.set(event_target_value(&event))
                    />
                </div>
            </div>
            {move || {
                (no_dir.get() && files.get().is_empty())
                    .then(|| view! { <div class="muted">{t("logs.no_dir")}</div> })
            }}
            {move || {
                (!no_dir.get() && files.get().is_empty())
                    .then(|| view! { <div class="muted">{t("logs.no_files")}</div> })
            }}
            {move || {
                (!files.get().is_empty() && selected.get().is_empty())
                    .then(|| view! { <div class="muted">{t("logs.pick")}</div> })
            }}
            <Show when=move || !selected.get().is_empty()>
                <crate::log_view::FollowLog content=Signal::derive(move || lines.get().join("\n")) />
            </Show>
        </div>
    }
}
