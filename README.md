# anilili-metadata

Static anime metadata — title details, episode lists, and cross-database id mapping — aggregated
from [AniList](https://anilist.co) and [TMDB](https://www.themoviedb.org), served over the jsDelivr
CDN with no server-side logic.

Rebuilt daily by a GitHub Action. Roughly 20,800 titles and 135,000 episodes.

## Data access

```
https://cdn.jsdelivr.net/gh/pommieyewear/anilili-metadata@main/data
```

| File | Contents |
| --- | --- |
| `index.json` | Flat catalogue of every title — id, title, poster, year, status, format, episode count, score, popularity, genres |
| `id-map.json` | AniList id → TMDB / MAL / Kitsu / AniDB / Simkl |
| `airing.json` | Titles with a scheduled next episode |
| `genres.json`, `years.json` | Distinct values, for building filters |
| `stats.json` | Totals by format and status, plus `last_updated` |
| `sync-status.json` | What generated the tree and when |
| `anime/{shard}/{id}/index.json` | Full detail for one title |
| `anime/{shard}/{id}/episodes.json` | Episode titles, overviews, air dates, stills, runtimes |
| `mappings/mal/{shard}/{malId}.json` | MyAnimeList id → AniList id |

`shard` is `Math.floor(id / 1000)` — for per-title files it shards on the AniList id, for the MAL
mappings on the MyAnimeList id.

```javascript
const BASE = "https://cdn.jsdelivr.net/gh/pommieyewear/anilili-metadata@main/data";

const catalog = await fetch(`${BASE}/index.json`).then(r => r.json());

const detailUrl = id => `${BASE}/anime/${Math.floor(id / 1000)}/${id}/index.json`;
const episodeUrl = id => `${BASE}/anime/${Math.floor(id / 1000)}/${id}/episodes.json`;

const episodes = await fetch(episodeUrl(171018)).then(r => r.json()); // DAN DA DAN
```

## Two things worth knowing before you rely on it

**Episodes are numbered by position, not by TMDB's `episode_number`.** TMDB keeps some multi-season
runs as one long season — Dandadan's two seasons as a single season of 24, Re:Zero's four as one of
85 — and numbers the second season 13..24 where AniList, the streaming providers and viewers all
call them 1..12. Each title's `episodes.json` is renumbered from 1 so it lines up with everything
else.

**A missing `episodes.json` is an answer, not a gap.** It means no TMDB record could be matched to
that AniList entry with enough confidence to publish. That is deliberate: a wrong match does not
show up as a missing thumbnail, it shows up as another show's episodes illustrating this one. An
unaired sequel gets no episode list rather than its predecessor's.

Where an episode count is known, it is binding — every title with an AniList episode count has
exactly that many episodes here.

## Rebuilding

```bash
pip install -r scripts/requirements.txt
python scripts/konoha_build.py catalog          # walk AniList, ~25 min
python scripts/konoha_build.py episodes         # TMDB episode lists, ~20 min
python scripts/konoha_build.py emit --out data
```

`episodes` needs a TMDB v4 read token in `TMDB_READ_TOKEN`. `scripts/test_konoha_build.py` covers
the title and season matching rules; run it after touching either.

The daily workflow only re-asks TMDB about titles that can still change. Use the **full** input on
a manual run to rebuild everything.

## Notes

Not affiliated with AniList or TMDB. This product uses the TMDB API but is not endorsed or
certified by TMDB. Metadata belongs to its respective sources; this repository is a cache.
