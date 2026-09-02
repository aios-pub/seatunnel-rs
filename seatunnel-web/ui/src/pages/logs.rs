// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Node log viewer: lists the engine's daily rolling log files and tails
//! them with level/substring filters and a download link.

use crate::api;
use crate::app::{auto_refresh, use_polling};
use crate::i18n::t;
use crate::ui::ErrorBanner;
use leptos::prelude::*;
use leptos::task::spawn_local;
use wasm_bindgen::JsCast;

#[component]
pub fn Logs() -> impl IntoView {
    let (files, set_files) = RwSignal::new_local(Vec::<String>::new()).split();
    let (no_dir, set_no_dir) = RwSignal::new_local(false).split();
    let (selected, set_selected) = RwSignal::new_local(String::new()).split();
    let (level, set_level) = RwSignal::new_local(String::new()).split();
    let (query, set_query) = RwSignal::new_local(String::new()).split();
    let (tail, set_tail) = RwSignal::new_local(2000u32).split();
    let (content, set_content) = RwSignal::new_local(None::<api::LogContent>).split();
    let (error, set_error) = RwSignal::new_local(None::<String>).split();
    let auto_scroll = RwSignal::new(true);

    // File list: fetched once plus on manual refresh.
    {
        let fetch_files = move || {
            spawn_local(async move {
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
        // Refetch the list when "Refresh now" is pressed.
        let bump = crate::app::refresh_bump();
        Effect::new(move || {
            let _tick = bump.get();
            fetch_files();
        });
    }

    // Content polling: only while a file is selected (and auto-refresh on).
    {
        let fetch = move || {
            let name = selected.get_untracked();
            if name.is_empty() {
                return;
            }
            let level = level.get_untracked();
            let query = query.get_untracked();
            let tail = tail.get_untracked();
            spawn_local(async move {
                match api::log_file(&name, tail, &level, &query).await {
                    Ok(value) => set_content.set(Some(value)),
                    Err(err) => set_error.set(Some(err)),
                }
            })
        };
        {
            let fetch = fetch.clone();
            Effect::new(move || {
                let _ = selected.get();
                fetch();
            });
        }
        use_polling(move || {
            if auto_refresh().get_untracked() {
                fetch();
            }
        });
    }

    // Keep the newest line in view while auto-scroll is on.
    Effect::new(move || {
        content.get();
        if !auto_scroll.get() {
            return;
        }
        let Some(doc) = window().document() else { return };
        if let Some(node) = doc.query_selector(".log-box").unwrap() {
            let element: web_sys::HtmlElement = node.unchecked_into();
            element.set_scroll_top(element.scroll_height());
        }
    });

    view! {
        <ErrorBanner message=Signal::derive(move || error.get()) />
        <div class="panel">
            <h2>{t("logs.title")}</h2>
            {move || {
                (no_dir.get() && files.get().is_empty())
                    .then(|| view! { <div class="muted">{t("logs.no_dir")}</div> })
            }}
            {move || {
                (!no_dir.get() && files.get().is_empty())
                    .then(|| view! { <div class="muted">{t("logs.no_files")}</div> })
            }}
            {move || {
                (!files.get().is_empty()).then(|| {
                    view! {
                        <div class="toolbar">
                            <div class="field">
                                <label>{t("logs.select")}</label>
                                <select
                                    class="inline-select"
                                    on:change=move |event| set_selected.set(event_target_value(&event))
                                >
                                    <option value="">{t("logs.pick")}</option>
                                    {files
                                        .get()
                                        .iter()
                                        .rev()
                                        .map(|name| {
                                            let value = name.clone();
                                            view! { <option value=value>{name.clone()}</option> }
                                        })
                                        .collect::<Vec<_>>()}
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
                                    on:input=move |event| set_query.set(event_target_value(&event))
                                />
                            </div>
                            <div class="field">
                                <label>{t("logs.tail")}</label>
                                <select
                                    class="inline-select"
                                    on:change=move |event| {
                                        set_tail.set(event_target_value(&event).parse().unwrap_or(2000));
                                    }
                                >
                                    <option value="500">"500"</option>
                                    <option value="2000" selected=true>"2000"</option>
                                    <option value="10000">"10000"</option>
                                </select>
                            </div>
                            {move || {
                                let name = selected.get();
                                (!name.is_empty()).then(|| {
                                    let href = api::log_file_download_url(
                                        &name,
                                        tail.get_untracked(),
                                        &level.get_untracked(),
                                        &query.get_untracked(),
                                    );
                                    view! {
                                        <a class="btn" href=href>{t("logs.download")}</a>
                                    }
                                })
                            }}
                        </div>
                        <div class="toolbar">
                            <label class="log-autoscroll">
                                <input
                                    type="checkbox"
                                    prop:checked=move || auto_scroll.get()
                                    on:change=move |event| {
                                        auto_scroll.set(event_target_checked(&event));
                                    }
                                />
                                {t("jd.autoscroll")}
                            </label>
                            <span class="muted">
                                {move || {
                                    content
                                        .get()
                                        .map(|c| {
                                            crate::i18n::tf(
                                                "jd.lines_title",
                                                &[&c.name, &c.lines.len().to_string()],
                                            )
                                        })
                                        .unwrap_or_default()
                                }}
                            </span>
                        </div>
                    }
                })
            }}
            {move || {
                content
                    .get()
                    .filter(|_| !selected.get().is_empty())
                    .map(|content| {
                        view! {
                            <pre class="log-box log-tall">
                                {if content.lines.is_empty() {
                                    t("jd.no_logs")
                                } else {
                                    content.lines.join("\n")
                                }}
                            </pre>
                        }
                            .into_any()
                    })
                    .unwrap_or_else(|| view! { <div class="muted">{t("logs.pick")}</div> }.into_any())
            }}
        </div>
    }
}
