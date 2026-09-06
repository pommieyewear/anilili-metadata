"""Build the anime metadata dataset the app reads in place of AlokRepo/Konoha.

Konoha was a third-party static mirror of AniList + TMDB served over jsDelivr. It stopped: its
`sync-status.json` still reads `lastFinishedYear: 2010`, `stats.json` was last written 2026-06-08
and `airing.json` was last checked 2026-05-19. Anything that aired since is thin or absent, which
is why `TmdbClient` exists at all — it fills the gaps Konoha left, one live request at a time, for
every viewer.

This rebuilds the same tree from the primary sources so the gaps stop being gaps. The output layout
is byte-for-byte the shape `KonohaClient` already parses, so pointing the app at it is a base-URL
change and nothing else.

The TMDB half is a deliberate port of `TmdbClient.kt`, not a fresh implementation. That file
encodes matching rules that were learned from real failures — an exact episode count binds harder
than a matching year, a season marker has to come off the title before searching, and the bundled
id-map's own `confidence` cannot be believed (it rates Dandadan a HIGH match for a 2024 Chinese
drama). Re-deriving those here would mean re-learning them. Where the two must agree, the Kotlin is
the source of truth and this follows it; see `pick_season`, `pick_group` and `titles_match`.

Stages are separate commands because they fail differently and the expensive one must be resumable:

    catalog     Walk AniList for every anime. ~20,800 titles, ~25 min (AniList is the slow one).
    episodes    Ask TMDB for episode lists. ~15,000 titles, ~20 min at 8 workers. Resumable.
    emit        Write the publishable tree from what the first two cached.
    assets      Copy the bundled files into app/src/main/assets/.

A daily refresh is `catalog` + `episodes --airing-only` + `emit`, which only touches titles that
can still change.

Usage:
    scripts/.venv/Scripts/python.exe scripts/konoha_build.py catalog
    scripts/.venv/Scripts/python.exe scripts/konoha_build.py episodes --airing-only
    scripts/.venv/Scripts/python.exe scripts/konoha_build.py emit --out build/konoha/data
    scripts/.venv/Scripts/python.exe scripts/konoha_build.py assets --out build/konoha/data
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import sys
import threading
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORK = ROOT / "build" / "konoha"
CATALOG_FILE = WORK / "catalog.json"
EPISODE_DIR = WORK / "episodes"
ASSETS = ROOT / "app/src/main/assets"

ANILIST_API = "https://graphql.anilist.co"
TMDB_API = "https://api.themoviedb.org/3"
TMDB_STILL_BASE = "https://image.tmdb.org/t/p/w500"

# Fribb's anime-lists: a community cross-reference pairing an AniList id with its AniDB, MAL, Kitsu,
# Simkl, TVDB and TMDB ids, and — the part nothing else states — which TMDB *season* that AniList
# entry is. Rebuilt weekly from AniDB's anime-list, public, no key.
#
# It is the answer to the question `pick_season` otherwise has to guess at from episode counts and
# air years. It covers 91% of the catalogue and states a season for about a third of it; the
# heuristic still runs for the rest, and still runs when what it claims does not check out.
FRIBB_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json"
FRIBB_FILE = WORK / "fribb-mini.json"
FRIBB_MAX_AGE_S = 3 * 24 * 60 * 60

# Formats whose titles have an episode list worth fetching. A movie has one "episode" and TMDB
# indexes it under /movie, which carries no stills per part; asking costs two requests for nothing.
EPISODIC_FORMATS = {"TV", "TV_SHORT", "ONA", "OVA", "SPECIAL"}

# AniList stops paging at 5000 entries, so 100 pages of 50 is the hard ceiling for any one filter.
MAX_PARTITION_PAGES = 100

# Requests held back from the published budget. AniList advertises 90/min but degrades to 30
# without changing the header, so pacing that spends the advertised budget exactly still 429s.
RATE_BUFFER = 5

# Seconds between AniList requests. 2.0 is the degraded ceiling of 30/min; starting there costs a
# few minutes on a healthy day and saves an hour on a degraded one, because a 429 is not a cheap
# retry — it is ~55s of Retry-After plus a wait to the window reset.
MIN_INTERVAL = 2.0
INTERVAL_STEP = 0.5
MAX_INTERVAL = 6.0

# Walked one broadcast year at a time, because a straight paged walk cannot reach the end of the
# catalogue: AniList refuses any request whose offset passes 5000 entries ("Page depth exceeds
# maximum allowed for API requests"), which at 50 per page is page 101 and AniList id ~7528 — about
# a quarter of what exists. There is no `id_greater` on Media to window by, but `startDate_greater`
# and `startDate_lesser` are accepted, and no single year comes close to 5000 titles, so each year
# pages to exhaustion on its own.
#
# `pageInfo.total` is not usable as a count here: it reports the 5000 cap for every filter, so the
# walk stops on `hasNextPage` rather than on a total.
#
# A year partition only sees titles that have a start date. Announcements that have none are picked
# up by a separate NOT_YET_RELEASED sweep, which is why $status is on the same query.
CATALOG_QUERY = """
query ($page: Int!, $perPage: Int!, $greater: FuzzyDateInt, $lesser: FuzzyDateInt, $status: MediaStatus) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage currentPage }
    media(
      type: ANIME
      sort: ID
      startDate_greater: $greater
      startDate_lesser: $lesser
      status: $status
    ) {
      id
      idMal
      title { romaji english native }
      description(asHtml: false)
      coverImage { large color }
      bannerImage
      format
      status
      season
      seasonYear
      episodes
      duration
      genres
      averageScore
      popularity
      isAdult
      startDate { year month day }
      nextAiringEpisode { episode airingAt }
    }
  }
}
"""


# --------------------------------------------------------------------------------------------
# Title matching — ported from TmdbClient.kt. Keep in step with it.
# --------------------------------------------------------------------------------------------

WHITESPACE = re.compile(r"\s+")

SEASON_MARKERS = [
    re.compile(r"\b\d+(st|nd|rd|th)\s+season\b", re.I),
    re.compile(r"\bseason\s*\d+\b", re.I),
    re.compile(r"\bpart\s*\d+\b", re.I),
    re.compile(r"\bcour\s*\d+\b", re.I),
    re.compile(r"\bfinal\s+season\b", re.I),
    re.compile(r"\s+(ii|iii|iv|v|vi|vii|viii|ix|x)\s*$", re.I),
]

# Below this a containment test stops meaning anything — "one" is inside a great many titles.
MIN_TITLE_LENGTH = 6

# TMDB's own type for the grouping that splits a run into broadcast seasons.
SEASONS_GROUP_TYPE = 6
# Re:Zero carries eight groupings; trying them all would cost more than the stills are worth.
MAX_GROUPS_TRIED = 2


def series_search_query(raw: str | None) -> str | None:
    """A title as the name of its *series* rather than of one season.

    TMDB indexes the show and splits it into seasons underneath, so a season's own name finds
    nothing: "Youjo Senki II" returns no results where "Youjo Senki" returns the show it is the
    second season of. Which season then gets used is decided separately, by count and year.
    """
    if not raw or not raw.strip():
        return None
    text = raw
    for marker in SEASON_MARKERS:
        text = marker.sub(" ", text)
    text = WHITESPACE.sub(" ", text).strip()
    return text or None


def normalize_title(raw: str | None) -> str:
    """Titles reduced to what two catalogues can be expected to agree on.

    Punctuation, spacing and case never survive the trip between AniList and TMDB — "Dandadan"
    against "Dan Da Dan" is the normal case, not the exception.
    """
    base = series_search_query(raw) or ""
    return "".join(ch for ch in base.lower() if ch.isalnum())


def titles_match(entry: dict, candidate: dict) -> bool:
    """Whether a TMDB record is plausibly the same show as an AniList one.

    Deliberately strict about the thing that goes wrong. A mismatch does not show up as a missing
    thumbnail, which a viewer shrugs at, but as another show's episodes illustrating this one.
    """
    titles = entry.get("title") or {}
    ours = [
        normalize_title(titles.get("romaji")),
        normalize_title(titles.get("english")),
        normalize_title(titles.get("native")),
    ]
    ours = [t for t in ours if len(t) >= MIN_TITLE_LENGTH]
    theirs = [normalize_title(candidate.get("name")), normalize_title(candidate.get("original_name"))]
    theirs = [t for t in theirs if len(t) >= MIN_TITLE_LENGTH]
    if not ours or not theirs:
        return False
    return any(mine == other or mine in other or other in mine for mine in ours for other in theirs)


def _year_of(date: str | None) -> int | None:
    if not date or len(date) < 4:
        return None
    try:
        return int(date[:4])
    except ValueError:
        return None


def is_sequel(entry: dict) -> bool:
    """Whether the AniList title names itself a later season, part or cour.

    Used only to withdraw the "lone season is unambiguous" fallback below. A title that survives
    `series_search_query` unchanged carries no season marker and is taken to be the whole series.
    """
    titles = entry.get("title") or {}
    for raw in (titles.get("romaji"), titles.get("english")):
        if raw and raw.strip() and series_search_query(raw) != raw.strip():
            return True
    return False


def pick_season(
    seasons: list[dict],
    wanted_episodes: int | None,
    wanted_year: int | None,
    sequel: bool = False,
) -> dict | None:
    """The TMDB season holding the AniList season being built, or None when a group split is needed.

    An exact episode count is the strongest signal available and the year is only a tie-break. A
    season of the wrong length is refused even when its year is the only one that fits: TMDB holds
    Dandadan's two AniList seasons as one season of 24 aired in 2024, and matching on year alone
    would hand the twelve-episode first season a twenty-four episode run.
    """
    real = [s for s in seasons if (s.get("season_number") or 0) > 0 and (s.get("episode_count") or 0) > 0]
    if not real:
        return None
    if wanted_episodes is not None:
        same_count = [s for s in real if s.get("episode_count") == wanted_episodes]
        for season in same_count:
            if _year_of(season.get("air_date")) == wanted_year:
                return season
        return same_count[0] if len(same_count) == 1 else None
    if wanted_year is not None:
        same_year = [s for s in real if _year_of(s.get("air_date")) == wanted_year]
        if len(same_year) == 1:
            return same_year[0]
    # A series with exactly one season and no count to go on is unambiguous by construction — but
    # only when the AniList entry is the series. For a title that names itself a later season, the
    # lone TMDB season is the *whole run*, and taking it is how "Dandadan 3rd Season" ends up with
    # seasons one and two's twenty-four episodes attached to a show that has not aired. AniList has
    # no episode count for an unannounced season, so nothing further up catches this.
    if sequel:
        return None
    return real[0] if len(real) == 1 else None


def whole_series_seasons(entry: dict, seasons: list[dict], sequel: bool) -> list[dict]:
    """Every TMDB season, in broadcast order, when the AniList entry covers the whole run.

    An endless series is one AniList entry holding every episode ever broadcast, while TMDB splits
    it into broadcast seasons. Picking one of those picks a fraction: One Piece is 23 TMDB seasons
    and 1,181 episodes, its AniList entry states no total because it is still running, and matching
    on the start year selected season 1 alone — 61 episodes ending in 2001, leaving every episode
    after that with no title and no still.

    No episode count and no season marker means the entry *is* the series, so the whole run is the
    answer. Empty when that does not hold, which leaves the ordinary season matching to decide:

    - A stated count binds to one season and is the stronger signal (see `pick_season`).
    - A named sequel is one season of a longer run, and the run is the one thing it must not get.
    - A series TMDB already keeps as a single season has nothing to concatenate — Sazae-san's 2,650
      and Detective Conan's 1,212 arrive that way and are unaffected either way.
    """
    if entry.get("episodes") is not None or sequel:
        return []
    numbered = sorted(
        (s for s in seasons if (s.get("season_number") or 0) > 0 and (s.get("episode_count") or 0) > 0),
        key=lambda s: s["season_number"],
    )
    return numbered if len(numbered) > 1 else []


def pick_group(groups: list[dict], wanted_episodes: int | None, wanted_year: int | None) -> dict | None:
    """The run of a "Seasons" episode group that holds the AniList season being built."""
    real = [g for g in groups if g.get("episodes")]
    if not real:
        return None

    def first_year(group: dict) -> int | None:
        episodes = group.get("episodes") or []
        return _year_of(episodes[0].get("air_date")) if episodes else None

    if wanted_episodes is not None:
        same_count = [g for g in real if len(g.get("episodes") or []) == wanted_episodes]
        for group in same_count:
            if first_year(group) == wanted_year:
                return group
        return same_count[0] if len(same_count) == 1 else None
    if wanted_year is not None:
        same_year = [g for g in real if first_year(g) == wanted_year]
        if len(same_year) == 1:
            return same_year[0]
    return None


def to_episodes(raw: list[dict]) -> list[dict]:
    """A run of TMDB episodes in Konoha's episode shape, numbered by position not by TMDB.

    Position is what survives both shapes. Where TMDB keeps a season of its own the two agree, but
    inside a merged run the second season's episodes are numbered 13..24 while every other
    catalogue — AniList, the stream providers, the viewer — calls them 1..12.
    """
    out = []
    for index, episode in enumerate(raw):
        still = episode.get("still_path")
        out.append(
            {
                "number": float(index + 1),
                "title": (episode.get("name") or None),
                "overview": (episode.get("overview") or None),
                "air_date": (episode.get("air_date") or None),
                "still": (TMDB_STILL_BASE + still) if still else None,
                "runtime": episode.get("runtime"),
            }
        )
    return out


# --------------------------------------------------------------------------------------------
# AniList
# --------------------------------------------------------------------------------------------


class AniList:
    """Paged AniList reader that spends its quota rather than holding a fixed rate.

    AniList publishes the budget on every response (`X-RateLimit-Remaining` and `-Reset`), and the
    degraded ceiling is 30/min against a normal 90. Reacting only to a 429 costs a full minute of
    timeout, so this glides toward the reset as the budget runs down instead.
    """

    def __init__(self, session: requests.Session, min_interval: float = MIN_INTERVAL) -> None:
        self.session = session
        self.remaining: int | None = None
        self.reset_at: float = 0.0
        self.last_request: float = 0.0
        # Adapts upward on every 429 and never comes back down within a run. The published budget
        # cannot be used to derive this: AniList degrades from 90/min to 30/min without changing
        # `X-RateLimit-Limit`, so pacing off the header alone spends three times the real budget
        # and 429s on essentially every request. The floor is what actually holds the rate.
        self.interval = min_interval

    def _pace(self) -> None:
        elapsed = time.time() - self.last_request
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        if self.remaining is None:
            return
        window = max(self.reset_at - time.time(), 0.0)
        if self.remaining <= RATE_BUFFER and window > 0:
            print(f"  quota exhausted, sleeping {window + 1:.0f}s to window reset", flush=True)
            time.sleep(window + 1)

    def fetch(self, page: int, per_page: int, variables: dict) -> dict:
        for attempt in range(6):
            self._pace()
            response = self.session.post(
                ANILIST_API,
                json={
                    "query": CATALOG_QUERY,
                    "variables": {"page": page, "perPage": per_page, **variables},
                },
                timeout=45,
            )
            self.last_request = time.time()
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset = response.headers.get("X-RateLimit-Reset")
            if remaining is not None:
                self.remaining = int(remaining)
            if reset is not None:
                self.reset_at = float(reset)

            if response.status_code == 429:
                wait = float(response.headers.get("Retry-After", 60)) + 1
                self.interval = min(self.interval + INTERVAL_STEP, MAX_INTERVAL)
                print(
                    f"  429, waiting {wait:.0f}s and slowing to {self.interval:.1f}s/request",
                    flush=True,
                )
                time.sleep(wait)
                continue
            if response.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            if response.status_code == 400:
                # A malformed query is not retryable, and the GraphQL body says what is wrong where
                # raise_for_status only says "Bad Request".
                raise RuntimeError(f"AniList rejected the query: {response.text[:500]}")
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise RuntimeError(f"AniList error on {variables} page {page}: {body['errors']}")
            return body["data"]["Page"]
        raise RuntimeError(f"AniList {variables} page {page} failed after retries")


def cmd_catalog(args: argparse.Namespace) -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Anilili-konoha-build (AniList client 45552)",
            # AniList refuses a request carrying neither a Referer nor an Authorization header,
            # and does it with a "temporarily disabled" 403 that reads like an outage. The value
            # is not inspected — see the same header in AniListClient.kt.
            "Referer": "android-app://com.miruronative",
        }
    )
    anilist = AniList(session, args.min_interval)

    # Resume from whatever a previous run left behind rather than re-walking it. A full pull is
    # ~600 requests against a 90/min budget, so an interrupted run is worth continuing.
    by_id: dict[int, dict] = {}
    if CATALOG_FILE.exists() and args.resume:
        for entry in json.loads(CATALOG_FILE.read_text(encoding="utf-8")):
            by_id[entry["id"]] = entry
        print(f"resuming from {len(by_id)} cached titles", flush=True)

    def drain(label: str, variables: dict) -> int:
        """Page one partition to exhaustion, returning how many titles it had not seen before."""
        added = 0
        page = 1
        while True:
            data = anilist.fetch(page, args.per_page, variables)
            media = data.get("media") or []
            for entry in media:
                # Overwrite rather than skip. A refresh exists to pick up a status that moved to
                # RELEASING, an episode count that grew and the next airing slot — all on titles
                # already cached. Keeping the first answer would make every run after the first a
                # no-op for precisely the titles that change.
                if entry["id"] not in by_id:
                    added += 1
                by_id[entry["id"]] = entry
            if not (data.get("pageInfo") or {}).get("hasNextPage"):
                break
            page += 1
            if page > MAX_PARTITION_PAGES:
                # Never reached by a year of anime, but a silent truncation here would be a hole in
                # the catalogue that nothing downstream could detect.
                print(f"  !! {label} exceeded {MAX_PARTITION_PAGES} pages — partition is too coarse")
                break
        if added:
            print(f"  {label}: +{added} (total {len(by_id)})", flush=True)
        return added

    current_year = datetime.now(timezone.utc).year
    partitions = list(range(args.from_year, current_year + 3))
    try:
        for year in partitions:
            drain(str(year), {"greater": year * 10000, "lesser": year * 10000 + 1232})
        # Announcements with no start date belong to no year partition, so they are swept by status.
        drain("unscheduled", {"status": "NOT_YET_RELEASED"})
    except KeyboardInterrupt:
        print("\ninterrupted — saving what was fetched", flush=True)

    entries = sorted(by_id.values(), key=lambda e: e["id"])
    CATALOG_FILE.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")
    print(f"\nwrote {len(entries)} titles to {CATALOG_FILE}")
    return 0


class FribbEntry(typing.NamedTuple):
    """One AniList id's cross-references, as Fribb states them."""

    tmdb_id: int | None
    tmdb_season: int | None
    tvdb_id: int | None
    tvdb_season: int | None
    mal_id: int | None
    kitsu_id: int | None
    anidb_id: int | None
    simkl_id: int | None


def load_fribb(refresh: bool = False) -> dict[int, FribbEntry]:
    """Fribb's cross-reference, indexed by AniList id. ~6 MB, cached for three days.

    `episode_offset` is deliberately not read. It is AniDB's offset into a *TVDB* season and Fribb
    copies it to the tmdb field unchanged, which does not survive the trip: `.hack//Liminality`
    (AniList 299) is four episodes at offset 1 of TMDB 8864 season 0, and the four it actually owns
    are that season's entries 1, 2, 3 and 5 — no single offset produces them under either reading.
    So specials keep the ordinary matching, which at worst gives them nothing rather than confidently
    giving them another OVA's titles.
    """
    WORK.mkdir(parents=True, exist_ok=True)
    stale = (
        refresh
        or not FRIBB_FILE.exists()
        or time.time() - FRIBB_FILE.stat().st_mtime > FRIBB_MAX_AGE_S
    )
    if stale:
        print(f"downloading {FRIBB_URL}", flush=True)
        response = requests.get(FRIBB_URL, timeout=120)
        response.raise_for_status()
        FRIBB_FILE.write_bytes(response.content)

    index: dict[int, FribbEntry] = {}
    for row in json.loads(FRIBB_FILE.read_text(encoding="utf-8")):
        anilist_id = row.get("anilist_id")
        if not anilist_id:
            continue
        # themoviedb_id is an object for TV and an array for film; only the TV side is single-valued
        # and only the TV side has seasons, so a movie mapping is left to the ordinary matching.
        raw_tmdb = row.get("themoviedb_id")
        tmdb_id = raw_tmdb.get("tv") if isinstance(raw_tmdb, dict) else None
        season = row.get("season") or {}
        index[anilist_id] = FribbEntry(
            tmdb_id=tmdb_id if isinstance(tmdb_id, int) else None,
            tmdb_season=season.get("tmdb"),
            tvdb_id=row.get("tvdb_id") if isinstance(row.get("tvdb_id"), int) else None,
            tvdb_season=season.get("tvdb"),
            mal_id=row.get("mal_id"),
            kitsu_id=row.get("kitsu_id"),
            anidb_id=row.get("anidb_id"),
            simkl_id=row.get("simkl_id"),
        )
    return index


def previous_id_map(out: pathlib.Path | None = None) -> dict[str, dict]:
    """The last id-map this pipeline produced, for the fields it cannot regenerate.

    Kitsu, AniDB and Simkl ids came from Konoha's own matching and there is no source here that can
    rebuild them, so they are carried forward rather than dropped — losing them would make the file
    worse than the one it replaces.

    The published tree is preferred over the bundled asset because they diverge as soon as the first
    refresh lands: in CI the app's assets directory does not exist at all, and on a dev machine it
    holds whatever shipped in the last APK rather than the newest run.
    """
    for candidate in ((out / "id-map.json") if out else None, ASSETS / "id-map.json"):
        if candidate and candidate.exists():
            return json.loads(candidate.read_text(encoding="utf-8"))
    return {}


def load_catalog() -> list[dict]:
    if not CATALOG_FILE.exists():
        raise SystemExit(f"No catalogue at {CATALOG_FILE}. Run `konoha_build.py catalog` first.")
    return json.loads(CATALOG_FILE.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------------------------
# TMDB
# --------------------------------------------------------------------------------------------


class Tmdb:
    def __init__(self, token: str) -> None:
        self.token = token
        self.local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
            )
            self.local.session = session
        return session

    def get(self, path: str) -> dict | None:
        separator = "&" if "?" in path else "?"
        url = f"{TMDB_API}{path}{separator}language=en-US"
        for attempt in range(5):
            try:
                response = self._session().get(url, timeout=30)
            except requests.RequestException:
                time.sleep(1 + attempt)
                continue
            # 404 is an ordinary answer: a series can simply have no episode groups.
            if response.status_code == 404:
                return None
            if response.status_code == 429:
                time.sleep(float(response.headers.get("Retry-After", 2)) + 1)
                continue
            if response.status_code >= 500:
                time.sleep(1 + attempt)
                continue
            if not response.ok:
                return None
            return response.json()
        return None

    def resolve_series(self, entry: dict, hint_id: int | None) -> dict | None:
        """The TMDB series for an AniList entry, checked against its titles before it is believed."""
        if hint_id:
            candidate = self.get(f"/tv/{hint_id}")
            if candidate and titles_match(entry, candidate):
                return candidate
        titles = entry.get("title") or {}
        queries: list[str] = []
        for raw in (titles.get("romaji"), titles.get("english")):
            for candidate in (raw.strip() if raw else None, series_search_query(raw)):
                if candidate and candidate not in queries:
                    queries.append(candidate)
        for query in queries:
            found = self.get(f"/search/tv?query={requests.utils.quote(query)}")
            for result in (found or {}).get("results", []):
                if titles_match(entry, result):
                    return self.get(f"/tv/{result['id']}")
        return None

    def episodes_for(
        self,
        entry: dict,
        hint_id: int | None,
        fribb: FribbEntry | None = None,
    ) -> tuple[list[dict], int | None]:
        """Episode rows for one AniList title, plus the TMDB series id they came from."""
        wanted_episodes = entry.get("episodes")

        # Fribb names the season outright, which is the one thing the heuristics below cannot do,
        # so it is tried first — but checked, not believed. What it states is a logical season that
        # TMDB does not always have: it calls DAN DA DAN Season 2 "season 2" of TMDB 240411, and
        # that series carries a single season of 24. So the season is fetched, and it is only
        # accepted when it comes back the length AniList says the season is. Anything else falls
        # through to the matching below, which is what found the right answer for that title.
        if fribb and fribb.tmdb_id and fribb.tmdb_season:
            payload = self.get(f"/tv/{fribb.tmdb_id}/season/{fribb.tmdb_season}")
            raw = (payload or {}).get("episodes") or []
            if raw and (wanted_episodes is None or len(raw) == wanted_episodes):
                return to_episodes(raw), fribb.tmdb_id

        # Fribb's id is a better starting point than the old bundled map's even when it names no
        # season: it is anime-specific and rebuilt weekly, where the bundled map is a frozen copy of
        # a dead mirror. It still has to survive titles_match like any other candidate.
        series = self.resolve_series(entry, fribb.tmdb_id if fribb and fribb.tmdb_id else hint_id)
        if not series:
            return [], None
        series_id = series.get("id")
        start = entry.get("startDate") or {}
        wanted_year = entry.get("seasonYear") or start.get("year")

        sequel = is_sequel(entry)
        seasons = series.get("seasons") or []

        # An endless series is one AniList entry covering every episode ever broadcast, and TMDB
        # splits it into broadcast seasons. Picking one of those is picking a fraction: One Piece is
        # 23 TMDB seasons and 1,181 episodes, its AniList entry states no total because it is still
        # running, and matching on the start year selected season 1 alone — 61 episodes ending in
        # 2001, with every episode after that left without a title or a still.
        #
        # No count and no season marker means the entry *is* the series, so the whole run is the
        # answer and the seasons are concatenated in broadcast order. Titles TMDB already keeps as
        # one season (Sazae-san's 2,650, Detective Conan's 1,212) are unaffected — there is nothing
        # to concatenate — and a named sequel is excluded because for it the run is exactly what it
        # must not be given.
        numbered = whole_series_seasons(entry, seasons, sequel)
        if numbered:
            run: list[dict] = []
            for season in numbered:
                payload = self.get(f"/tv/{series_id}/season/{season['season_number']}")
                run.extend((payload or {}).get("episodes") or [])
            if run:
                return to_episodes(run), series_id

        summary = pick_season(seasons, wanted_episodes, wanted_year, sequel)
        if summary:
            season = self.get(f"/tv/{series_id}/season/{summary['season_number']}")
            raw = (season or {}).get("episodes") or []
            if raw:
                return to_episodes(raw), series_id

        # No season lines up, which is what a series TMDB keeps as one long run looks like. TMDB's
        # own answer is an episode group, and the "Seasons" kind splits the run back into the
        # seasons the rest of the world numbers from — carrying the stills with it.
        groups = [
            g
            for g in ((self.get(f"/tv/{series_id}/episode_groups") or {}).get("results") or [])
            if g.get("type") == SEASONS_GROUP_TYPE and (g.get("group_count") or 0) > 1
        ]
        groups.sort(key=lambda g: g.get("episode_count") or 0, reverse=True)
        for group in groups[:MAX_GROUPS_TRIED]:
            detail = self.get(f"/tv/episode_group/{group['id']}")
            matched = pick_group((detail or {}).get("groups") or [], wanted_episodes, wanted_year)
            if matched:
                return to_episodes(matched.get("episodes") or []), series_id
        return [], series_id


def tmdb_token(explicit: str | None) -> str:
    """The TMDB v4 read token, from the same places the Gradle build reads it.

    Never committed: the build takes it from private Gradle user properties or the environment, and
    so does this. See app/build.gradle.kts.
    """
    if explicit:
        return explicit
    for name in ("TMDB_READ_TOKEN", "TMDB_TOKEN"):
        if os.environ.get(name):
            return os.environ[name]
    props = pathlib.Path.home() / ".gradle" / "gradle.properties"
    if props.exists():
        for line in props.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.strip().startswith("tmdbReadToken"):
                return line.split("=", 1)[1].strip()
    raise SystemExit(
        "No TMDB read token. Set TMDB_READ_TOKEN, or put tmdbReadToken in ~/.gradle/gradle.properties."
    )


def cmd_episodes(args: argparse.Namespace) -> int:
    catalog = load_catalog()
    EPISODE_DIR.mkdir(parents=True, exist_ok=True)
    tmdb = Tmdb(tmdb_token(args.token))

    fribb = load_fribb(args.refresh_fribb)
    print(f"fribb cross-reference: {len(fribb)} AniList ids", flush=True)

    # The old id-map is only a fallback hint now, for the 9% of the catalogue Fribb has never
    # heard of. Its own `confidence` is not trusted — every hint is re-checked by titles_match.
    hints: dict[int, int] = {}
    for key, value in previous_id_map(pathlib.Path(args.out) if args.out else None).items():
        if value.get("tmdb") and value.get("tmdb_type") != "movie":
            hints[int(key)] = value["tmdb"]

    targets = []
    for entry in catalog:
        if entry.get("format") not in EPISODIC_FORMATS:
            continue
        if args.airing_only and entry.get("status") not in {"RELEASING", "NOT_YET_RELEASED"}:
            continue
        path = EPISODE_DIR / f"{entry['id']}.json"
        if path.exists() and not args.refresh and not args.airing_only:
            continue
        targets.append(entry)
    if args.limit:
        targets = targets[: args.limit]

    print(f"{len(targets)} titles to fetch (workers={args.workers})", flush=True)
    done = 0
    matched = 0
    lock = threading.Lock()

    def work(entry: dict) -> None:
        nonlocal done, matched
        try:
            episodes, series_id = tmdb.episodes_for(
                entry, hints.get(entry["id"]), fribb.get(entry["id"])
            )
        except Exception as error:  # noqa: BLE001 - one bad title must not end an hours-long run
            with lock:
                done += 1
            print(f"  !! {entry['id']} {error}", flush=True)
            return
        payload = {"tmdb_id": series_id, "episodes": episodes}
        (EPISODE_DIR / f"{entry['id']}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        with lock:
            done += 1
            if episodes:
                matched += 1
            if done % 100 == 0:
                print(f"  {done}/{len(targets)} ({matched} matched)", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(work, targets))

    print(f"\ndone: {done} fetched, {matched} with episodes")
    return 0


# --------------------------------------------------------------------------------------------
# Emit
# --------------------------------------------------------------------------------------------


def _write(path: pathlib.Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def _slug(title: str, anilist_id: int) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")
    return f"{base}-{anilist_id}" if base else str(anilist_id)


def cmd_emit(args: argparse.Namespace) -> int:
    catalog = load_catalog()
    out = pathlib.Path(args.out)

    # Read before --clean runs, which deletes the very file this carries forward from. Getting this
    # order wrong silently drops every Kitsu, AniDB and Simkl id on the first cleaned rebuild.
    carried = previous_id_map(out)
    fribb = load_fribb(args.refresh_fribb)
    print(f"fribb cross-reference: {len(fribb)} AniList ids")

    if out.exists() and args.clean:
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    index_rows: list[dict] = []
    id_map: dict[str, dict] = {}
    airing: list[dict] = []
    genres: dict[str, int] = {}
    years: dict[str, int] = {}
    formats: dict[str, int] = {}
    statuses: dict[str, int] = {}
    episode_count = 0

    for entry in catalog:
        anilist_id = entry["id"]
        titles = entry.get("title") or {}
        display = titles.get("english") or titles.get("romaji") or titles.get("native") or ""
        images = entry.get("coverImage") or {}
        start = entry.get("startDate") or {}
        year = entry.get("seasonYear") or start.get("year")
        status = entry.get("status")
        fmt = entry.get("format")

        index_rows.append(
            {
                "id": anilist_id,
                "title": display,
                "slug": _slug(display, anilist_id),
                "poster": images.get("large"),
                "poster_color": images.get("color"),
                "year": year,
                "status": status,
                "format": fmt,
                "episodes": entry.get("episodes"),
                "score": entry.get("averageScore"),
                "popularity": entry.get("popularity"),
                "genres": entry.get("genres") or [],
            }
        )

        for value, bucket in ((fmt, formats), (status, statuses)):
            if value:
                bucket[value] = bucket.get(value, 0) + 1
        for genre in entry.get("genres") or []:
            genres[genre] = genres.get(genre, 0) + 1
        if year:
            years[str(year)] = years.get(str(year), 0) + 1

        # Per-title detail, in the shape KonohaClient.KonohaDetail parses.
        detail = {
            "ids": {"anilist": anilist_id, "mal": entry.get("idMal")},
            "titles": {
                "romaji": titles.get("romaji"),
                "english": titles.get("english"),
                "native": titles.get("native"),
            },
            "format": fmt,
            "status": status,
            "season": entry.get("season"),
            "season_year": year,
            "episodes": entry.get("episodes"),
            "duration": entry.get("duration"),
            "description": entry.get("description"),
            "genres": entry.get("genres") or [],
            "score_anilist": entry.get("averageScore"),
            "popularity": entry.get("popularity"),
            "images": {
                "poster": images.get("large"),
                "poster_color": images.get("color"),
                "banner": entry.get("bannerImage"),
            },
        }
        shard = anilist_id // 1000
        _write(out / "anime" / str(shard) / str(anilist_id) / "index.json", detail)

        cached = EPISODE_DIR / f"{anilist_id}.json"
        episodes_path = out / "anime" / str(shard) / str(anilist_id) / "episodes.json"
        tmdb_id = None
        if cached.exists():
            payload = json.loads(cached.read_text(encoding="utf-8"))
            episodes = payload.get("episodes") or []
            tmdb_id = payload.get("tmdb_id")
            if episodes:
                _write(episodes_path, episodes)
                episode_count += len(episodes)
            elif episodes_path.exists():
                # A title that used to resolve and no longer does must lose its file, not keep the
                # old one. Without this a correction can never reach a device: the run that stopped
                # matching writes nothing, the previous run's episodes.json stays on the CDN, and
                # the wrong episode list outlives the fix that was supposed to remove it.
                episodes_path.unlink()

        previous = carried.get(str(anilist_id), {})
        cross = fribb.get(anilist_id)
        mal_id = entry.get("idMal")
        # Kitsu, AniDB and Simkl come from Fribb now rather than being carried forward from a dead
        # mirror. Konoha had almost none of them — AniList 1 shipped `kitsu: null, simkl: null` —
        # and there was no way to regenerate what it did have, so every rebuild could only preserve
        # or lose them. Fribb states them for most of the catalogue and is rebuilt weekly.
        # Carry-forward stays as a floor for the 9% Fribb has never heard of.
        id_map[str(anilist_id)] = {
            # A TMDB id this run verified through titles_match, else Fribb's, else what was there.
            # `confidence` is advisory only; the app re-checks any id before it puts stills on a
            # page, which is why a merely-stated id is still worth writing.
            "tmdb": tmdb_id or (cross.tmdb_id if cross else None) or previous.get("tmdb"),
            "tmdb_type": "tv" if (tmdb_id or (cross and cross.tmdb_id)) else previous.get("tmdb_type"),
            "mal": mal_id or (cross.mal_id if cross else None),
            "kitsu": (cross.kitsu_id if cross else None) or previous.get("kitsu"),
            "anidb": (cross.anidb_id if cross else None) or previous.get("anidb"),
            "simkl": (cross.simkl_id if cross else None) or previous.get("simkl"),
            # TheTVDB groups a series the way viewers talk about it — one series id, one season per
            # AniList entry — which AniList itself has no concept of. It is what the season chain on
            # the detail page is built from; see DetailData.seasons.
            "tvdb": cross.tvdb_id if cross else None,
            "tvdb_season": cross.tvdb_season if cross else None,
            "confidence": "HIGH" if tmdb_id else ("MAPPED" if cross and cross.tmdb_id else previous.get("confidence")),
            "updated_at": now,
        }
        if mal_id:
            _write(out / "mappings" / "mal" / str(mal_id // 1000) / f"{mal_id}.json", {"anilist_id": anilist_id})

        upcoming = entry.get("nextAiringEpisode")
        if upcoming:
            airing.append(
                {
                    "id": anilist_id,
                    "title": display,
                    "next_episode": upcoming.get("episode"),
                    "next_airing_at": datetime.fromtimestamp(
                        upcoming["airingAt"], timezone.utc
                    ).isoformat().replace("+00:00", "Z"),
                    "last_checked": now,
                }
            )

    _write(out / "index.json", index_rows)
    _write(out / "id-map.json", id_map)
    _write(out / "airing.json", airing)
    _write(out / "genres.json", sorted(genres))
    _write(out / "years.json", sorted(years, reverse=True))
    _write(
        out / "stats.json",
        {
            "total": len(index_rows),
            "formats": formats,
            "statuses": statuses,
            "episodes": episode_count,
            "last_updated": now,
        },
    )
    _write(
        out / "sync-status.json",
        {"source": "anilist+tmdb", "generator": "scripts/konoha_build.py", "last_updated": now},
    )

    print(f"titles      {len(index_rows)}")
    print(f"episodes    {episode_count}")
    print(f"airing      {len(airing)}")
    print(f"tmdb linked {sum(1 for v in id_map.values() if v['tmdb'])}")
    print(f"\nwrote {out}")
    return 0


def cmd_assets(args: argparse.Namespace) -> int:
    """Copy the two files the APK bundles.

    index.json is the offline catalogue LocalCatalog reads when AniList cannot be reached;
    id-map.json is the MAL/TMDB cross-reference KonohaClient loads at startup.

    The bundle is allowed to be smaller than the published tree, and should be. The tree is what a
    device queries and needs to be complete; the bundle is a floor for the day AniList returns 403
    to everyone, it costs APK size on every install forever, and it goes stale the moment it ships.
    Trimming what nobody would search for in an anime app buys back most of the growth: the full
    catalogue is 20,778 titles and 6.9 MB against the 9,978 and 4.5 MB that shipped before, and
    2,616 of the new ones are MUSIC — anime music videos, which are not a streaming fallback.
    """
    out = pathlib.Path(args.out)
    for name in ("index.json", "id-map.json"):
        if not (out / name).exists():
            raise SystemExit(f"{out / name} missing — run `emit` first.")

    dropped_formats = {value.strip().upper() for value in args.drop_formats.split(",") if value.strip()}
    rows = json.loads((out / "index.json").read_text(encoding="utf-8"))
    kept = [
        row
        for row in rows
        if row.get("format") not in dropped_formats
        and (row.get("popularity") or 0) >= args.min_popularity
    ]

    target = ASSETS / "index.json"
    before = target.stat().st_size if target.exists() else 0
    target.write_text(
        json.dumps(kept, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    print(
        f"index.json: {len(rows):,} titles -> {len(kept):,} kept "
        f"({before:,} -> {target.stat().st_size:,} bytes)"
    )

    # The id-map is not trimmed with it. It is keyed by AniList id and read on a lookup for a title
    # the viewer already reached, so a row for a title missing from the offline catalogue still gets
    # used — dropping those rows would break MAL resolution for exactly the obscure titles that
    # need it most.
    target = ASSETS / "id-map.json"
    before = target.stat().st_size if target.exists() else 0
    shutil.copyfile(out / "id-map.json", target)
    print(f"id-map.json: {before:,} -> {target.stat().st_size:,} bytes")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    catalog = commands.add_parser("catalog", help="page AniList for every anime")
    catalog.add_argument("--per-page", type=int, default=50, help="AniList caps this at 50")
    catalog.add_argument(
        "--from-year", type=int, default=1900, help="first broadcast year to walk (default 1900)"
    )
    catalog.add_argument(
        "--min-interval",
        type=float,
        default=MIN_INTERVAL,
        help=f"seconds between requests, raised automatically on 429 (default {MIN_INTERVAL})",
    )
    catalog.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="start from id 0 instead of continuing the cached walk",
    )
    catalog.set_defaults(func=cmd_catalog, resume=True)

    episodes = commands.add_parser("episodes", help="fetch TMDB episode lists")
    episodes.add_argument("--token", help="TMDB v4 read token (default: env or gradle properties)")
    episodes.add_argument(
        "--out",
        default=str(WORK / "data"),
        help="published tree to take TMDB hints from, if one exists yet",
    )
    episodes.add_argument("--workers", type=int, default=8)
    episodes.add_argument("--limit", type=int, default=0)
    episodes.add_argument("--refresh", action="store_true", help="re-fetch titles already cached")
    episodes.add_argument(
        "--refresh-fribb", action="store_true", help="re-download the cross-reference before running"
    )
    episodes.add_argument(
        "--airing-only",
        action="store_true",
        help="only RELEASING/NOT_YET_RELEASED titles — the daily refresh",
    )
    episodes.set_defaults(func=cmd_episodes)

    emit = commands.add_parser("emit", help="write the publishable tree")
    emit.add_argument("--out", default=str(WORK / "data"))
    emit.add_argument("--clean", action="store_true", help="delete the output tree first")
    emit.add_argument(
        "--refresh-fribb", action="store_true", help="re-download the cross-reference before running"
    )
    emit.set_defaults(func=cmd_emit)

    assets = commands.add_parser("assets", help="copy bundled files into app assets")
    assets.add_argument("--out", default=str(WORK / "data"))
    assets.add_argument(
        "--drop-formats",
        default="MUSIC",
        help="comma-separated formats to leave out of the bundled catalogue (default MUSIC)",
    )
    assets.add_argument(
        "--min-popularity",
        type=int,
        default=0,
        help="leave titles below this AniList popularity out of the bundled catalogue",
    )
    assets.set_defaults(func=cmd_assets)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
