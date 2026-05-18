# Cricket Australia: A Nation's Game

**FIT2179 Data Visualisation 2 · Monash University · Semester 1, 2026**

**Live page:** https://bjai0003-dot.github.io/FIT2179/DV2/

---

## Overview

A scrollytelling data visualisation essay covering Australia's cricket history across four chapters:

1. **Australia on the World Stage** — head-to-head record against every nation, ICC rankings
2. **The Trophy Cabinet** — ICC tournaments from 1975 to 2025
3. **The Ashes** — every Test ever played, venues, and top performers
4. **The Big Bash** — franchise map, attendance growth, win heatmap

## Visualisations (12 charts, 3 maps)

| # | ID | Idiom | Map? |
|---|----|-------|------|
| 01 | `01_head_to_head_map.vg.json` | Choropleth + proportional symbol | ✅ |
| 02 | `02_rankings_over_time.vg.json` | Multi-series line chart |  |
| 03 | `03_kpi_sparkline.vg.json` | Sparkline |  |
| 04 | `04_trophy_timeline.vg.json` | Lollipop timeline |  |
| 05 | `05_wc_titles_radial.vg.json` | Radial bar chart |  |
| 06 | `06_format_polar.vg.json` | Polar area (coxcomb) |  |
| 07 | `07_ashes_strip.vg.json` | Matrix / result strip |  |
| 08 | `08_ashes_venues_map.vg.json` | Geographic dot map (dual) | ✅ |
| 09 | `09_ashes_top_performers.vg.json` | Connected dot plot |  |
| 10 | `10_bbl_franchises_map.vg.json` | Proportional symbol map | ✅ |
| 11 | `11_bbl_attendance.vg.json` | Annotated area chart |  |
| 12 | `12_bbl_heatmap.vg.json` | Heatmap matrix |  |

All Vega-Lite specs are in `js/specs/` and formatted for human readability.

## Data Sources

| File | Source | URL |
|------|--------|-----|
| `head_to_head.csv` | ESPN Cricinfo Statsguru | https://stats.espncricinfo.com |
| `rankings_over_time.csv` | ICC Test Championship history | https://www.icc-cricket.com |
| `icc_trophies.csv` | ICC tournament records | https://www.icc-cricket.com |
| `wc_titles.csv` | ICC ODI World Cup records | https://www.icc-cricket.com |
| `win_by_format.csv` | ESPN Cricinfo format records | https://stats.espncricinfo.com |
| `ashes_matches.csv` | CricketArchive Ashes history | https://www.cricketarchive.com |
| `ashes_venues.csv` | ESPN Cricinfo ground records | https://stats.espncricinfo.com |
| `ashes_top_performers.csv` | ESPN Cricinfo player stats | https://stats.espncricinfo.com |
| `bbl_franchises.csv` | Cricket Australia / BBL official | https://www.cricketaustralia.com.au |
| `bbl_attendance.csv` | Cricket Australia season reports | https://www.cricketaustralia.com.au |
| `bbl_wins.csv` | BBL official records | https://www.bigbash.com.au |
| `kpi_wins_trend.csv` | Derived from ESPN Cricinfo | https://stats.espncricinfo.com |

## Running Locally

```bash
# Any static file server works. E.g. Python:
cd DV2/
python3 -m http.server 8000
# Then open http://localhost:8000
```

Note: opening `index.html` directly via `file://` will block CSV loads due to browser CORS restrictions. Use a local server.

## Deploying to GitHub Pages

```bash
# From repo root
git add DV2/
git commit -m "Add DV2 cricket dashboard"
git push origin main

# Then in GitHub repo → Settings → Pages:
# Source: Deploy from branch → main → / (root)
# Your URL: https://bjai0003-dot.github.io/FIT2179/DV2/
```

## AI Acknowledgement

Claude (Anthropic) was used to assist with code structure, Vega-Lite specification debugging, and copy editing. All data sourcing, chart design decisions, narrative analysis, and final implementation are the author's own. Used in accordance with Monash University FIT2179 AI use policy.

## Author

Somanshu · Monash University · FIT2179 · Semester 1, 2026
