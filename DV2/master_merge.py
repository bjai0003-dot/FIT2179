"""
MASTER CRICSHEET MERGE SCRIPT
==============================
Processes Tests, ODIs, T20Is folders and produces all datasets needed for DV2.

USAGE:
1. Update the 3 folder paths below to match your machine
2. Run: python master_merge.py
3. It will create all output CSVs in the same directory as this script
"""

import os
import csv
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════════
# UPDATE THESE PATHS TO MATCH YOUR MAC
# ═══════════════════════════════════════════════════════════════════════
FOLDERS = {
    "Test":  "/Users/bhavyajain/Downloads/tests_male_csv2",
    "ODI":   "/Users/bhavyajain/Downloads/odis_male_csv2",
    "T20I":  "/Users/bhavyajain/Downloads/t20is_male_csv2",
}
# ═══════════════════════════════════════════════════════════════════════


def parse_info(filepath):
    """Parse a Cricsheet _info.csv file and extract match metadata + player rosters."""
    match_id = os.path.basename(filepath).replace("_info.csv", "")
    data = {"match_id": match_id}
    teams = []
    players = []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",", 2)
            if len(parts) < 3:
                continue
            key = parts[1].strip()
            val = parts[2].strip().strip('"')

            if key == "team":
                teams.append(val)
            elif key == "season":
                data["season"] = val
            elif key == "date" and "date" not in data:
                data["date"] = val
            elif key == "event":
                data["event"] = val
            elif key == "match_number":
                data["match_number"] = val
            elif key == "venue":
                data["venue"] = val
            elif key == "city":
                data["city"] = val
            elif key == "winner":
                data["winner"] = val
            elif key == "winner_runs":
                data["margin"] = val + " runs"
            elif key == "winner_wickets":
                data["margin"] = val + " wickets"
            elif key == "outcome":
                data["winner"] = val  # "draw", "no result", etc.
            elif key == "player":
                rest = parts[2].split(",", 1)
                if len(rest) == 2:
                    players.append({
                        "match_id": match_id,
                        "team": rest[0].strip(),
                        "player": rest[1].strip()
                    })

    data["team1"] = teams[0] if len(teams) > 0 else ""
    data["team2"] = teams[1] if len(teams) > 1 else ""
    return data, players


def aggregate_batting(folder, format_name, aus_only=False):
    """Aggregate runs per player from ball-by-ball CSVs."""
    player_runs = defaultdict(lambda: {"runs": 0, "balls": 0, "matches": set(), "team": "", "format": format_name})

    for fname in sorted(os.listdir(folder)):
        if fname.endswith("_info.csv") or not fname.endswith(".csv"):
            continue
        match_id = fname.replace(".csv", "")
        filepath = os.path.join(folder, fname)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    striker = row.get("striker", "")
                    team = row.get("batting_team", "")
                    runs = int(row.get("runs_off_bat", 0) or 0)

                    if aus_only and team != "Australia":
                        continue

                    key = (striker, team, format_name)
                    player_runs[key]["runs"] += runs
                    player_runs[key]["balls"] += 1
                    player_runs[key]["matches"].add(match_id)
                    player_runs[key]["team"] = team
        except Exception as e:
            print(f"  Warning: Error reading {fname}: {e}")
            continue

    return player_runs


# ═══════════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════════

all_matches = []
all_players = []
all_batting = {}

for format_name, folder in FOLDERS.items():
    if not os.path.exists(folder):
        print(f"WARNING: Folder not found: {folder}")
        print(f"  Please update the path for {format_name} at the top of this script.")
        continue

    print(f"Processing {format_name} from {folder}...")

    # Parse info files
    info_count = 0
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith("_info.csv"):
            continue
        filepath = os.path.join(folder, fname)
        match_data, players = parse_info(filepath)
        match_data["format"] = format_name
        all_matches.append(match_data)
        for p in players:
            p["format"] = format_name
            all_players.append(p)
        info_count += 1

    print(f"  Parsed {info_count} match info files")

    # Aggregate batting (Australia players only to keep it fast)
    print(f"  Aggregating batting stats (this may take a minute)...")
    batting = aggregate_batting(folder, format_name, aus_only=False)
    all_batting.update(batting)
    print(f"  Found {len(batting)} player-team-format combos")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 1: international_matches.csv
# ═══════════════════════════════════════════════════════════════════════
fields = ["match_id", "format", "season", "date", "event", "match_number",
          "team1", "team2", "winner", "margin", "venue", "city"]

with open("international_matches.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for m in all_matches:
        writer.writerow({k: m.get(k, "") for k in fields})

print(f"\n✅ international_matches.csv: {len(all_matches)} matches")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 2: player_career_batting.csv (all players, all teams)
# ═══════════════════════════════════════════════════════════════════════
batting_rows = []
for (player, team, fmt), d in all_batting.items():
    batting_rows.append({
        "player": player,
        "team": team,
        "format": fmt,
        "total_runs": d["runs"],
        "balls_faced": d["balls"],
        "matches": len(d["matches"]),
        "strike_rate": round(d["runs"] / d["balls"] * 100, 2) if d["balls"] > 0 else 0
    })
batting_rows.sort(key=lambda x: x["total_runs"], reverse=True)

with open("player_career_batting.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["player", "team", "format", "total_runs", "balls_faced", "matches", "strike_rate"])
    writer.writeheader()
    writer.writerows(batting_rows)

print(f"✅ player_career_batting.csv: {len(batting_rows)} player-team-format rows")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 3: aus_player_totals.csv (Australian batters, all formats combined)
# ═══════════════════════════════════════════════════════════════════════
aus_totals = defaultdict(lambda: {"runs": 0, "balls": 0, "matches": 0})
for (player, team, fmt), d in all_batting.items():
    if team == "Australia":
        aus_totals[player]["runs"] += d["runs"]
        aus_totals[player]["balls"] += d["balls"]
        aus_totals[player]["matches"] += len(d["matches"])

aus_rows = []
for player, d in aus_totals.items():
    aus_rows.append({
        "player": player,
        "team": "Australia",
        "total_runs": d["runs"],
        "balls_faced": d["balls"],
        "matches": d["matches"],
        "strike_rate": round(d["runs"] / d["balls"] * 100, 2) if d["balls"] > 0 else 0
    })
aus_rows.sort(key=lambda x: x["total_runs"], reverse=True)

with open("aus_player_totals.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["player", "team", "total_runs", "balls_faced", "matches", "strike_rate"])
    writer.writeheader()
    writer.writerows(aus_rows)

print(f"✅ aus_player_totals.csv: {len(aus_rows)} Australian batters")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 4: tournament_matches.csv (World Cups, Champions Trophy only)
# ═══════════════════════════════════════════════════════════════════════
tournament_keywords = ["World Cup", "Champions Trophy", "World T20", "ICC"]
tournament_matches = []
for m in all_matches:
    event = m.get("event", "")
    if any(kw.lower() in event.lower() for kw in tournament_keywords):
        tournament_matches.append(m)

with open("tournament_matches.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for m in tournament_matches:
        writer.writerow({k: m.get(k, "") for k in fields})

print(f"✅ tournament_matches.csv: {len(tournament_matches)} ICC tournament matches")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 5: ashes_matches.csv (AUS vs ENG Tests only)
# ═══════════════════════════════════════════════════════════════════════
ashes = []
for m in all_matches:
    if m.get("format") != "Test":
        continue
    teams = {m.get("team1", ""), m.get("team2", "")}
    if "Australia" in teams and "England" in teams:
        ashes.append(m)

with open("ashes_matches.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for m in ashes:
        writer.writerow({k: m.get(k, "") for k in fields})

print(f"✅ ashes_matches.csv: {len(ashes)} Ashes matches")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT 6: unique_venues.csv (all unique venues with cities)
# ═══════════════════════════════════════════════════════════════════════
venues = {}
for m in all_matches:
    v = m.get("venue", "")
    if v and v not in venues:
        venues[v] = m.get("city", "")

with open("unique_venues.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["venue", "city"])
    writer.writeheader()
    for v, c in sorted(venues.items()):
        writer.writerow({"venue": v, "city": c})

print(f"✅ unique_venues.csv: {len(venues)} unique venues")


# ═══════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════
print(f"""
{'='*60}
ALL DONE! Files created:
{'='*60}
1. international_matches.csv  — All matches with event/tournament info
2. player_career_batting.csv  — Every player's batting stats by format
3. aus_player_totals.csv      — Australian batters, all formats combined
4. tournament_matches.csv     — ICC World Cup & Champions Trophy matches
5. ashes_matches.csv          — Australia vs England Test matches
6. unique_venues.csv          — All venues with city names

These + your existing BBL files cover EVERY chart in your DV2.
{'='*60}
""")
