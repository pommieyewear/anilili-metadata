"""Checks that the TMDB matching ported into konoha_build.py still agrees with TmdbClient.kt.

Every case here is one the Kotlin names in a comment as having actually gone wrong. The two
implementations decide the same thing for the same title, and when they drift a viewer sees another
show's episodes illustrating this one — which is why the port is tested at all rather than trusted.

Plain asserts and no pytest: this runs in the same bare venv the release scripts use, and adding a
test dependency to publish an APK is not a trade worth making.

    scripts/.venv/Scripts/python.exe scripts/test_konoha_build.py
"""
from __future__ import annotations

import sys

from konoha_build import (
    is_sequel,
    normalize_title,
    pick_group,
    pick_season,
    series_search_query,
    titles_match,
    to_episodes,
    whole_series_seasons,
)

FAILURES: list[str] = []


def check(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        FAILURES.append(f"{label}\n    expected {expected!r}\n    got      {actual!r}")


def media(romaji: str | None = None, english: str | None = None, native: str | None = None) -> dict:
    return {"title": {"romaji": romaji, "english": english, "native": native}}


# -- series_search_query -----------------------------------------------------------------------
# TMDB indexes the series, not the season: "Youjo Senki II" returns nothing where "Youjo Senki"
# returns the show it is the second season of.
check("strips roman numeral", series_search_query("Youjo Senki II"), "Youjo Senki")
check("strips ordinal season", series_search_query("Kaguya-sama 2nd Season"), "Kaguya-sama")
check("strips numbered season", series_search_query("Overlord Season 4"), "Overlord")
check("strips part", series_search_query("Jujutsu Kaisen Part 2"), "Jujutsu Kaisen")
check("strips cour", series_search_query("Some Show Cour 2"), "Some Show")
check("strips final season", series_search_query("Attack on Titan Final Season"), "Attack on Titan")
check("leaves a plain title alone", series_search_query("Cowboy Bebop"), "Cowboy Bebop")
check("empty is None", series_search_query("   "), None)

# A roman numeral is only a season marker at the end of a title. Stripping it anywhere would maim
# names that legitimately contain one.
check("keeps interior numeral", series_search_query("Fate/stay night V Movie"), "Fate/stay night V Movie")

# -- normalize_title ---------------------------------------------------------------------------
# Punctuation, spacing and case never survive the trip between AniList and TMDB.
check("spacing folded", normalize_title("Dan Da Dan"), normalize_title("Dandadan"))
check("punctuation folded", normalize_title("Re:ZERO -Starting Life-"), "rezerostartinglife")

# -- titles_match ------------------------------------------------------------------------------
# The case the Kotlin calls out by id: the bundled map rates AniList 171018 (Dandadan) a
# HIGH-confidence match for TMDB 274861, a 2024 Chinese drama. Believing it would have decorated
# every episode row with that drama's stills.
check(
    "rejects the Dandadan mismatch",
    titles_match(media(romaji="Dandadan"), {"name": "Melody of Golden Age", "original_name": "群星闪耀时"}),
    False,
)
check(
    "accepts a spacing variant",
    titles_match(media(romaji="Dandadan"), {"name": "Dan Da Dan", "original_name": "ダンダダン"}),
    True,
)
check(
    "accepts containment",
    titles_match(media(english="Attack on Titan"), {"name": "Attack on Titan: Final Season"}),
    True,
)
# Below six characters a containment test stops meaning anything.
check(
    "refuses to match on a short title",
    titles_match(media(romaji="Air"), {"name": "Airplane Stories"}),
    False,
)
check("no titles is not a match", titles_match(media(), {"name": "Anything At All"}), False)

# -- pick_season -------------------------------------------------------------------------------
SEASONS = [
    {"season_number": 0, "episode_count": 5, "air_date": "2024-01-01"},  # specials, never picked
    {"season_number": 1, "episode_count": 12, "air_date": "2024-10-03"},
    {"season_number": 2, "episode_count": 12, "air_date": "2025-07-04"},
]
check("count plus year", pick_season(SEASONS, 12, 2025), SEASONS[2])
check("specials are never picked", pick_season(SEASONS, 5, 2024), None)

# The trap the Kotlin documents: TMDB holds Dandadan's two AniList seasons as one season of 24
# aired in 2024. A twelve-episode AniList season must not match it just because the year lines up,
# or every row of the second season gets the wrong still.
MERGED = [{"season_number": 1, "episode_count": 24, "air_date": "2024-10-03"}]
check("wrong length is refused even when the year fits", pick_season(MERGED, 12, 2024), None)

# A single season of the right length is still the answer when the year disagrees — that is a
# season that started in December or slipped.
SLIPPED = [{"season_number": 1, "episode_count": 12, "air_date": "2023-12-28"}]
check("lone right-length season survives a wrong year", pick_season(SLIPPED, 12, 2024), SLIPPED[0])

# Two seasons of the same length and neither year matching is genuinely ambiguous, so neither wins.
AMBIGUOUS = [
    {"season_number": 1, "episode_count": 12, "air_date": "2019-01-01"},
    {"season_number": 2, "episode_count": 12, "air_date": "2021-01-01"},
]
check("ambiguous count with no year match", pick_season(AMBIGUOUS, 12, 2024), None)
check("unambiguous single season with no count", pick_season(SLIPPED, None, None), SLIPPED[0])
check("no real seasons", pick_season([{"season_number": 0, "episode_count": 3}], 3, 2024), None)

# An unannounced sequel has no episode count on AniList, so nothing above catches it, and TMDB
# keeps the whole run as one season. Taking that lone season gave "Dandadan 3rd Season" seasons one
# and two's 24 episodes — a full episode list, with stills, for a show that has not aired.
check("lone season refused for a sequel", pick_season(MERGED, None, 2027, sequel=True), None)
check("lone season still taken for a series", pick_season(MERGED, None, 2027, sequel=False), MERGED[0])

# -- is_sequel ---------------------------------------------------------------------------------
check("ordinal season is a sequel", is_sequel(media(romaji="Dandadan 3rd Season")), True)
check("numbered season is a sequel", is_sequel(media(english="Black Clover Season 2")), True)
check("part is a sequel", is_sequel(media(romaji="Boruto Part 2")), True)
check("trailing numeral is a sequel", is_sequel(media(romaji="Youjo Senki II")), True)
# Sazae-san and Detective Conan have no AniList episode count either, but they are the series, not
# a season of one — the fallback has to keep working for them or they lose thousands of episodes.
check("plain title is not a sequel", is_sequel(media(romaji="Sazae-san")), False)
check("Detective Conan is not a sequel", is_sequel(media(english="Detective Conan")), False)

# -- whole_series_seasons ----------------------------------------------------------------------
# One Piece: AniList states no total because it is still running, TMDB splits it into 23 broadcast
# seasons, and matching on the 1999 start year selected season 1 alone — 61 episodes ending in
# 2001. Every episode after that reached a device with no title and no still, and the app quietly
# paid a live TMDB request per title to paper over it.
LONG_RUN = [
    {"season_number": 1, "episode_count": 61, "air_date": "1999-10-20"},
    {"season_number": 2, "episode_count": 16, "air_date": None},
    {"season_number": 3, "episode_count": 14, "air_date": None},
]
ongoing = {"title": {"romaji": "One Piece"}, "episodes": None}
check(
    "endless series takes every season",
    [s["season_number"] for s in whole_series_seasons(ongoing, LONG_RUN, sequel=False)],
    [1, 2, 3],
)
# A stated count binds to a single season and is the stronger signal, so the run is not taken.
counted = {"title": {"romaji": "Some Show"}, "episodes": 12}
check("a stated count wins", whole_series_seasons(counted, LONG_RUN, sequel=False), [])
# A named sequel is one season of a longer run; the run is the one thing it must not be given.
check("sequel never takes the run", whole_series_seasons(ongoing, LONG_RUN, sequel=True), [])
# Sazae-san and Detective Conan arrive as a single TMDB season, so there is nothing to concatenate
# and the ordinary matching handles them exactly as before.
SINGLE = [{"season_number": 1, "episode_count": 2650, "air_date": "1969-10-05"}]
check("single season is left alone", whole_series_seasons(ongoing, SINGLE, sequel=False), [])
# Specials are never part of the run.
check(
    "specials excluded from the run",
    [s["season_number"] for s in whole_series_seasons(ongoing, [{"season_number": 0, "episode_count": 9}] + LONG_RUN, sequel=False)],
    [1, 2, 3],
)

# -- pick_group --------------------------------------------------------------------------------
GROUPS = [
    {"name": "Season 1", "episodes": [{"air_date": "2024-10-03"}] * 12},
    {"name": "Season 2", "episodes": [{"air_date": "2025-07-04"}] * 12},
]
check("group by count and first air year", pick_group(GROUPS, 12, 2025), GROUPS[1])
check("group with no count match", pick_group(GROUPS, 24, 2025), None)
check("empty groups", pick_group([{"name": "x", "episodes": []}], 12, 2025), None)

# -- to_episodes -------------------------------------------------------------------------------
# Inside a merged run TMDB numbers the second season 13..24 while AniList, the providers and the
# viewer all call them 1..12. Position is what survives both shapes.
merged_run = [
    {"episode_number": 13, "name": "Thirteen", "still_path": "/a.jpg", "air_date": "2025-07-04", "overview": "o"},
    {"episode_number": 14, "name": "Fourteen", "still_path": None, "air_date": "2025-07-11", "overview": None},
]
rows = to_episodes(merged_run)
check("renumbered from position", [row["number"] for row in rows], [1.0, 2.0])
check("still becomes an absolute url", rows[0]["still"], "https://image.tmdb.org/t/p/w500/a.jpg")
check("absent still stays null", rows[1]["still"], None)
check("title carried", rows[0]["title"], "Thirteen")
check("air date carried", rows[1]["air_date"], "2025-07-11")

# An empty string is not a title, and shipping one would render a blank row rather than falling
# back to the episode number.
blank = to_episodes([{"episode_number": 1, "name": "", "still_path": "", "air_date": "", "overview": ""}])
check("blank title becomes null", blank[0]["title"], None)
check("blank still becomes null", blank[0]["still"], None)
check("blank air date becomes null", blank[0]["air_date"], None)


if FAILURES:
    print(f"{len(FAILURES)} failure(s):\n")
    for failure in FAILURES:
        print(f"  {failure}\n")
    sys.exit(1)
print("all matching checks passed")
