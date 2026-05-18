/**
 * main.js
 * FIT2179 Data Visualisation 2 — Australian Cricket Dashboard
 * Loads each Vega-Lite JSON spec and embeds it into the page via vega-embed.
 *
 * Charts embedded:
 *  #vis-headtohead   01 — head-to-head world map
 *  #vis-rankings     02 — ICC rankings over time
 *  #vis-spark-wins   03 — KPI sparkline (wins trend)
 *  #vis-trophies     04 — ICC trophy lollipop timeline
 *  #vis-wctitles     05 — ODI WC titles radial bar
 *  #vis-formats      06 — win rate by format (polar area)
 *  #vis-ashesstrip   07 — Ashes result strip
 *  #vis-ashesvenues  08 — Ashes venues map
 *  #vis-ashestop     09 — Ashes top performers connected dot
 *  #vis-bblmap       10 — BBL franchise map
 *  #vis-bblatt       11 — BBL attendance area chart
 *  #vis-bblheat      12 — BBL win heatmap
 */

/* ── Configuration ──────────────────────────────────────── */
const SPECS_DIR = "js/specs/";

// Vega-Embed shared options
const embedOpts = {
  actions: {
    export: true,
    source: true,   // exposes JSON spec link — required by assignment
    compiled: false,
    editor: false
  },
  renderer: "svg",
  tooltip: { theme: "dark" },
};

/* ── Chart registry ─────────────────────────────────────── */
const charts = [
  { containerId: "vis-headtohead",  spec: "01_head_to_head_map.vg.json",   opts: {} },
  { containerId: "vis-rankings",    spec: "02_rankings_over_time.vg.json", opts: {} },
  { containerId: "vis-spark-wins",  spec: "03_kpi_sparkline.vg.json",      opts: { actions: false } },
  { containerId: "vis-trophies",    spec: "04_trophy_timeline.vg.json",    opts: {} },
  { containerId: "vis-wctitles",    spec: "05_wc_titles_radial.vg.json",   opts: {} },
  { containerId: "vis-formats",     spec: "06_format_polar.vg.json",       opts: {} },
  { containerId: "vis-ashesstrip",  spec: "07_ashes_strip.vg.json",        opts: {} },
  { containerId: "vis-ashesvenues", spec: "08_ashes_venues_map.vg.json",   opts: {} },
  { containerId: "vis-ashestop",    spec: "09_ashes_top_performers.vg.json", opts: {} },
  { containerId: "vis-bblmap",      spec: "10_bbl_franchises_map.vg.json", opts: {} },
  { containerId: "vis-bblatt",      spec: "11_bbl_attendance.vg.json",     opts: {} },
  { containerId: "vis-bblheat",     spec: "12_bbl_heatmap.vg.json",        opts: {} },
];

/* ── Intersection observer: lazy-load charts on scroll ──── */
// Charts are rendered only when their container scrolls into view.
// This keeps initial load fast.
function observeAndEmbed(entry) {
  const el   = entry.target;
  const id   = el.id;
  const cfg  = charts.find(c => c.containerId === id);
  if (!cfg) return;

  const specUrl = SPECS_DIR + cfg.spec;
  const opts    = Object.assign({}, embedOpts, cfg.opts);

  vegaEmbed("#" + id, specUrl, opts)
    .then(result => {
      // Expose Vega view on the container for debugging
      el._vegaView = result.view;
    })
    .catch(err => {
      console.error(`[DV2] Failed to load spec for #${id}:`, err);
      el.innerHTML = `<p class="vis-error">⚠ Chart could not load. Check browser console.</p>`;
    });
}

/* ── Init on DOM ready ──────────────────────────────────── */
document.addEventListener("DOMContentLoaded", () => {

  // Set up IntersectionObserver
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        observeAndEmbed(entry);
        observer.unobserve(entry.target); // embed once only
      }
    });
  }, {
    rootMargin: "0px 0px 200px 0px", // start loading 200px before it enters view
    threshold: 0.01
  });

  // Observe every vis container
  charts.forEach(cfg => {
    const el = document.getElementById(cfg.containerId);
    if (el) {
      observer.observe(el);
    } else {
      console.warn(`[DV2] Container #${cfg.containerId} not found in DOM.`);
    }
  });

  // Hero: animate stat numbers in on load
  animateStatNumbers();
});

/* ── Stat number animation ──────────────────────────────── */
function animateStatNumbers() {
  const stats = document.querySelectorAll(".stat__num");
  stats.forEach((el, i) => {
    el.style.opacity = "0";
    el.style.transform = "translateY(16px)";
    el.style.transition = `opacity 0.5s ease ${i * 0.12}s, transform 0.5s ease ${i * 0.12}s`;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        el.style.opacity = "1";
        el.style.transform = "translateY(0)";
      });
    });
  });
}
