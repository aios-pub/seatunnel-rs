// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Entry point of the SeaTunnel web console (Leptos CSR).

mod api;
mod app;
mod charts;
mod fmt;
mod i18n;
mod log_view;
mod pages;
mod ui;

use leptos::prelude::*;
use wasm_bindgen::JsCast;

fn main() {
    // Root the console-wide signals before any reactive scope exists.
    app::init_globals();
    // Mount into the #app container (not body-append) so the shell layout
    // owns the viewport from the first paint. `forget` keeps the mount
    // alive: dropping the handle would unmount the app immediately.
    let host = document()
        .get_element_by_id("app")
        .expect("#app element missing from index.html");
    leptos::mount::mount_to(host.unchecked_into(), app::App).forget();
}
