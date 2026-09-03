// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Live-log viewer building blocks: a scroll-following log pane and
//! fullscreen plumbing shared by the job detail and node log pages.

use leptos::prelude::*;
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::JsCast;

/// A single log pane that follows the newest line until the user scrolls
/// away; a floating "back to bottom" button appears while detached and
/// following resumes automatically at the bottom.
#[component]
pub fn FollowLog(
    /// Full pane text (the caller joins its lines).
    #[prop(into)] content: Signal<String>,
) -> impl IntoView {
    let box_ref: NodeRef<leptos::html::Pre> = NodeRef::new();
    let (follow, set_follow) = RwSignal::new_local(true).split();

    let scroll_bottom = {
        let box_ref = box_ref.clone();
        move || {
            if let Some(el) = box_ref.get() {
                let el: web_sys::HtmlElement = el.unchecked_into();
                el.set_scroll_top(el.scroll_height());
            }
        }
    };

    // Detach as soon as the user scrolls away from the bottom; re-attach
    // when they come back. (The content Effect performs the same check on
    // every update, so this handler only makes the UI snappier.)
    let on_scroll = {
        let follow = follow;
        let set_follow = set_follow.clone();
        move |_| {
            let Some(el) = box_ref.get() else { return };
            let el: web_sys::HtmlElement = el.unchecked_into();
            let at_bottom = el.scroll_top() + el.client_height() >= el.scroll_height() - 40;
            if at_bottom != follow.get_untracked() {
                set_follow.set(at_bottom);
            }
        }
    };

    // On every content change, measure FIRST: if the pane sits at the
    // bottom the user is following — scroll to the new bottom; otherwise
    // they scrolled away (sync the flag so the "back to bottom" button
    // shows even if the scroll event was coalesced). Measuring instead of
    // relying on scroll events keeps this correct even when events are
    // delayed or coalesced.
    Effect::new({
        let box_ref = box_ref.clone();
        move || {
            content.track();
            if let Some(el) = box_ref.get() {
                let el: web_sys::HtmlElement = el.unchecked_into();
                let at_bottom =
                    el.scroll_top() + el.client_height() >= el.scroll_height() - 40;
                if at_bottom != follow.get_untracked() {
                    set_follow.set(at_bottom);
                }
                if at_bottom {
                    el.set_scroll_top(el.scroll_height());
                }
            }
        }
    });

    view! {
        <div class="follow-log">
            <pre class="log-box" node_ref=box_ref on:scroll=on_scroll>
                {move || content.get()}
            </pre>
            <Show when=move || !follow.get()>
                <button
                    class="follow-btn"
                    title=move || crate::i18n::t("logs.follow")
                    on:click=move |_| {
                        set_follow.set(true);
                        scroll_bottom();
                    }
                >
                    {move || crate::i18n::t("logs.follow")}
                </button>
            </Show>
        </div>
    }
}

/// Attach a window-level Escape handler while `active` is true; the
/// listener is removed when the flag flips false or the owner unmounts.
/// The window listener itself is registered on the UI thread only, so the
/// non-Send `Closure` is stashed in a thread-local for the cleanup hook.
pub fn use_escape_on(
    active: impl Fn() -> bool + Clone + 'static,
    action: impl Fn() + Clone + 'static,
) {
    thread_local! {
        static ESC_HANDLER: std::cell::RefCell<Option<Closure<dyn FnMut(web_sys::KeyboardEvent)>>> =
            const { std::cell::RefCell::new(None) };
    }
    Effect::new(move || {
        if !active() {
            return;
        }
        let handler =
            Closure::<dyn FnMut(web_sys::KeyboardEvent)>::new({
                let action = action.clone();
                move |ev: web_sys::KeyboardEvent| {
                    if ev.key() == "Escape" {
                        action();
                    }
                }
            });
        let callback = handler.as_ref().unchecked_ref();
        let _ = window().add_event_listener_with_callback("keydown", callback);
        ESC_HANDLER.with(|slot| *slot.borrow_mut() = Some(handler));
        on_cleanup(move || {
            ESC_HANDLER.with(|slot| {
                if let Some(handler) = slot.borrow_mut().take() {
                    let callback = handler.as_ref().unchecked_ref();
                    let _ = window().remove_event_listener_with_callback("keydown", callback);
                }
            });
        });
    });
}
