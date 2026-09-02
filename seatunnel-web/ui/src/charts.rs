// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.

//! Dependency-free SVG line charts for the console's time series.

use leptos::prelude::*;
use wasm_bindgen::JsValue;

/// One plotted series: `(ts_ms, value)` pairs, newest last.
pub struct Series {
    pub name: String,
    pub color: String,
    pub points: Vec<(f64, f64)>,
}

/// Default categorical palette for per-task / per-worker series.
pub const PALETTE: [&str; 8] = [
    "#2563eb", "#15803d", "#d97706", "#b91c1c", "#7c3aed", "#0891b2", "#db2777", "#4d7c0f",
];

const W: f64 = 600.0;
const H: f64 = 170.0;
const PAD_L: f64 = 46.0;
const PAD_R: f64 = 10.0;
const PAD_T: f64 = 8.0;
const PAD_B: f64 = 20.0;

fn fmt_clock(ms: f64) -> String {
    let date = js_sys::Date::new(&JsValue::from_f64(ms));
    date.to_locale_time_string("en-GB").as_string().unwrap_or_default()
}

fn fmt_value(v: f64) -> String {
    if v.abs() >= 100.0 {
        format!("{:.0}", v)
    } else if v.abs() >= 10.0 {
        format!("{:.1}", v)
    } else {
        format!("{:.2}", v)
    }
}

/// Multi-series line chart with a legend, a light grid and per-point hover
/// titles. Fixed internal resolution; scales to the container width.
#[component]
pub fn LineChart(
    /// Chart caption (rendered above the plot).
    #[prop(into)] title: String,
    series: Vec<Series>,
    /// Unit suffix shown next to the max y label (e.g. "rec/s", "ms", "%").
    #[prop(optional, into)] unit: String,
) -> impl IntoView {
    let y_max = series
        .iter()
        .flat_map(|s| s.points.iter().map(|(_, v)| *v))
        .fold(1.0_f64, f64::max)
        .max(1.0);
    let x_min = series
        .iter()
        .flat_map(|s| s.points.iter().map(|(t, _)| *t))
        .fold(f64::MAX, f64::min);
    let x_max = series
        .iter()
        .flat_map(|s| s.points.iter().map(|(t, _)| *t))
        .fold(f64::MIN, f64::max);
    let (x_min, x_max) = if x_min.is_finite() && x_max > x_min {
        (x_min, x_max)
    } else {
        // Degenerate window (no/one sample): render an empty frame.
        (0.0, 1.0)
    };

    let x = move |t: f64| PAD_L + (t - x_min) / (x_max - x_min) * (W - PAD_L - PAD_R);
    let y = move |v: f64| H - PAD_B - (v / y_max) * (H - PAD_T - PAD_B);

    let plot_h = H - PAD_T - PAD_B;

    let grid_y = [0.0, 0.5, 1.0];
    let y_label = format!("{}{}", fmt_value(y_max), if unit.is_empty() { String::new() } else { format!(" {}", unit) });
    let x_first = fmt_clock(x_min);
    let x_last = fmt_clock(x_max);

    let mut lines = Vec::new();
    let mut dots = Vec::new();
    for s in &series {
        if s.points.is_empty() {
            continue;
        }
        let path = s
            .points
            .iter()
            .map(|(t, v)| format!("{:.1},{:.1}", x(*t), y(*v)))
            .collect::<Vec<_>>()
            .join(" ");
        lines.push(view! {
            <polyline
                points=path
                fill="none"
                stroke=s.color.clone()
                stroke-width="1.6"
                stroke-linejoin="round"
            />
        });
        for (t, v) in &s.points {
            let title = format!("{} {} {}: {}", s.name, fmt_clock(*t), unit, fmt_value(*v));
            dots.push(view! {
                <circle cx=x(*t) cy=y(*v) r="2.2" fill=s.color.clone()>
                    <title>{title}</title>
                </circle>
            });
        }
    }

    let legend = series
        .iter()
        .filter(|s| !s.points.is_empty())
        .map(|s| {
            view! {
                <span class="chart-legend-item">
                    <span class="chart-swatch" style=format!("background: {}", s.color)></span>
                    {s.name.clone()}
                </span>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <div class="chart">
            <div class="chart-title">{title}</div>
            {if lines.is_empty() {
                view! { <div class="muted chart-empty">{crate::i18n::t("chart.collecting")}</div> }
                    .into_any()
            } else {
                view! {
                    <div>
                        <svg viewBox=format!("0 0 {} {}", W, H) preserveAspectRatio="xMidYMid meet">
                            {grid_y.iter().map(|f| {
                                let gy = PAD_T + (1.0 - f) * plot_h;
                                let label = fmt_value(f * y_max);
                                view! {
                                    <line x1=PAD_L y1=gy x2={W - PAD_R} y2=gy
                                        stroke="var(--border)" stroke-width="1" />
                                    <text x={PAD_L - 4.0} y={gy + 3.0} text-anchor="end"
                                        class="chart-tick">{label}</text>
                                }
                            }).collect::<Vec<_>>()}
                            {lines}
                            {dots}
                            <text x={PAD_L - 4.0} y={PAD_T + 3.0} text-anchor="end" class="chart-tick">
                                {y_label}
                            </text>
                            <text x=PAD_L y={H - 6.0} class="chart-tick">{x_first}</text>
                            <text x={W - PAD_R} y={H - 6.0} text-anchor="end" class="chart-tick">
                                {x_last}
                            </text>
                        </svg>
                        <div class="chart-legend">{legend}</div>
                    </div>
                }
                    .into_any()
            }}
        </div>
    }
}
