#!/usr/bin/env python3
"""Resolve 稀饭动漫 stream URLs ahead of time, so the app plays without asking the site.

The app's own lookup is two requests and a third to resolve a stream — measured on a Galaxy S25
at ~1,750ms for catalogue-to-playing. Reading one file from the CDN instead is 111-167ms cold and
nothing at all warm, because the app caches it. Precomputing is only defensible because these URLs
do not expire: re-resolved eight hours apart, seven of seven came back byte-identical, and they
carry no token, signature or `exp`.

They do move, once. The site keeps two layouts:

  finished   /{L}/{L}-{series}/[S{n}/]{NN}.mp4   verified back to 1986, stable for years
  airing     /temp/{YYMM}/… or /d/wo/{YYMM}/…    the staging area for the current year

A title stays in `temp` for its whole broadcast *year* -- a 2026-01 show was still there in
September -- and is filed into the archive at the rollover. The new path is not derivable: the
letter is the pinyin initial of the first character, but the folder is a hand-curated series name
(`G-关于我转生为史莱姆这档事系列` where the title reads 转生变成, with 系列 appended). So `temp`
entries are marked as such and the app heals them on the first playback failure rather than trusting
them forever.

    python lili_sources.py --repo . --top 10 [--write]
    python lili_sources.py --repo . --anilist 21 154587 [--write]
    python lili_sources.py --repo . --airing [--write]
"""
from __future__ import annotations

import argparse, json, os, re, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException

BASE = 'https://dm1.xfdm.pro'
UA = ('Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) '
      'Chrome/124.0.0.0 Mobile Safari/537.36')

COVER_ID = re.compile(r'/pic/cover/[^"\'\s)]*?/(\d+)_[^"\'\s)]*\.(?:jpg|jpeg|png|webp)', re.I)
WATCH_ANCHOR = re.compile(
    r"""<a\b[^>]*\bhref\s*=\s*['"](/watch/\d+/(\d+)/\d+\.html)['"][^>]*>([\s\S]*?)</a>""", re.I)
EPISODE_NUMBER = re.compile(r'第\s*0*(\d+)\s*[话話集]')
PLAYER_CONFIG = re.compile(r'player_\w+\s*=\s*(\{[\s\S]*?\})\s*[;<]', re.I)
TAGS = re.compile(r'<[^>]+>')

# Ten truncates. 进击的巨人 has thirteen entries and the endpoint returns the later seasons first,
# so season one -- the subject actually wanted -- fell off the end and the title read as absent.
SUGGEST_LIMIT = 50

# play.xfvod.pro is Cloudflare-fronted and sustained 28.7 Mbit/s; apn.moedot.net redirects into
# China Unicom's consumer cloud and managed 10.2 for the same byte range. Same file either way.
HOST_RANK = {'play.xfvod.pro': 0, 'dl.playxf.top': 1, 'apn.moedot.net': 2}


def fetch(path: str, tries: int = 3) -> str:
    url = path if path.startswith('http') else BASE + path
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Referer': f'{BASE}/',
                                                       'Accept-Encoding': 'identity'})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode('utf-8', 'replace')
        except (OSError, HTTPException):
            if attempt == tries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))
    return ''


def subject_path(bangumi_id: int, names: list[str]) -> str | None:
    """The site's page for a Bangumi subject, confirmed by the id in its cover filename."""
    for name in names[:3]:
        try:
            payload = json.loads(fetch(
                f'/index.php/ajax/suggest?mid=1&wd={urllib.parse.quote(name)}&limit={SUGGEST_LIMIT}'))
        except Exception:
            continue
        for row in payload.get('list') or []:
            hit = COVER_ID.search(row.get('pic') or '')
            if hit and hit.group(1) == str(bangumi_id):
                return f"/bangumi/{row['id']}.html"
    return None


def episodes(page: str) -> dict[int, dict[int, str]]:
    """channel -> {episode number -> watch path}, numbered from the label not the path."""
    out: dict[int, dict[int, str]] = {}
    for path, channel, label in WATCH_ANCHOR.findall(page):
        m = EPISODE_NUMBER.search(TAGS.sub('', label))
        if m:
            out.setdefault(int(channel), {})[int(m.group(1))] = path
    return out


def stream_url(watch_path: str) -> str | None:
    m = PLAYER_CONFIG.search(fetch(watch_path))
    if not m:
        return None
    try:
        url = json.loads(m.group(1)).get('url') or ''
    except Exception:
        return None
    url = url.replace('\\/', '/').strip()
    return url if url.startswith('http') else None


def layout_of(url: str) -> str:
    decoded = urllib.parse.unquote(url)
    return 'temp' if '/temp/' in decoded or '/d/wo/' in decoded else 'archive'


def best_channel(found: dict[int, dict[int, str]]) -> int | None:
    """The channel with the most episodes, and among those the one on the fastest host.

    Coverage first, because a channel missing episodes cannot be made up for by being quick. But
    the channels are mirrors of one file rather than a quality ladder, so where two carry the same
    episodes the host decides: play.xfvod.pro is Cloudflare-fronted and sustained 28.7 Mbit/s while
    apn.moedot.net redirects into China Unicom's consumer cloud and managed 10.2 for the same byte
    range. Ranking by channel number instead picked the slow one whenever both were complete, which
    is most airing titles.

    Learning the host costs one watch page per candidate, which is why only the channels already
    tied on coverage are sampled.
    """
    if not found:
        return None
    most = max(len(v) for v in found.values())
    tied = sorted(c for c in found if len(found[c]) == most)
    if len(tied) == 1:
        return tied[0]

    ranked = []
    for channel in tied:
        first = found[channel][min(found[channel])]
        host = ''
        url = stream_url(first)
        if url:
            host = (urllib.parse.urlsplit(url).hostname or '').lower()
        ranked.append((HOST_RANK.get(host, len(HOST_RANK)), channel))
    return min(ranked)[1]


def crawl(title: dict, concurrency: int, prior: dict | None = None) -> dict | None:
    bangumi = title['bangumi']
    path = subject_path(bangumi['id'], bangumi.get('search_names') or [])
    if not path:
        return None
    found = episodes(fetch(path))
    channel = best_channel(found)
    if channel is None:
        return None

    # An airing title gains one episode a week and keeps the rest, so re-resolving all of them to
    # discover the new one is most of a nightly crawl spent learning nothing. Episodes already
    # recorded are carried over and only the new ones are fetched -- the difference between a few
    # thousand requests a night and a few dozen.
    known: dict[str, str] = {}
    if prior and prior.get('subject') == path:
        base = prior.get('base', '')
        known = {k: base + v for k, v in (prior.get('episodes') or {}).items()}

    numbers = sorted(found[channel])
    wanted = [n for n in numbers if str(n) not in known]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        urls = list(pool.map(lambda n: stream_url(found[channel][n]), wanted))

    got = dict(known)
    got.update({str(n): u for n, u in zip(wanted, urls) if u})
    # An episode the site has dropped should not linger in the record forever.
    got = {k: v for k, v in got.items() if int(k) in found[channel]}
    if not got:
        return None
    layouts = {layout_of(u) for u in got.values()}
    base, suffixes = split_common_base(got)
    return {
        'provider': 'xfdm',
        'bangumi': bangumi['id'],
        'subject': path,
        # 'temp' anywhere means the whole entry is provisional: the site files a title away as a
        # unit at year rollover, so a mixed record will move as a unit too.
        'layout': 'temp' if 'temp' in layouts else 'archive',
        'crawled': time.strftime('%Y-%m-%d'),
        'base': base,
        'episodes': suffixes,
    }


def split_common_base(urls: dict[str, str]) -> tuple[str, dict[str, str]]:
    """The directory every episode shares, and what is left of each.

    A title's episodes differ only in the filename — `.../Z-咒术回战/01.mp4`, `02.mp4` — so storing
    the prefix once takes One Piece's 1,113 entries from 86KB to a fraction of it. That file is
    read to play a single episode, so its size is latency the viewer waits through.

    The prefix is cut at a `/` so a base is always a directory: truncating mid-filename would pair
    `.../无职0` with `1z.mp4` and `8.mp4`, which is correct but unreadable, and would break the
    moment one episode sat in a different folder.
    """
    values = list(urls.values())
    prefix = os.path.commonprefix(values)
    cut = prefix.rfind('/') + 1
    base = prefix[:cut]
    return base, {k: v[cut:] for k, v in urls.items()}


def read_json(path):
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


def select(repo: str, args) -> list[tuple[int, str]]:
    """(anilist id, path to its index.json) for whatever the caller asked to crawl."""
    index = read_json(os.path.join(repo, 'data', 'index.json'))
    by_id = {e['id']: e for e in index}
    if args.anilist:
        chosen = [i for i in args.anilist if i in by_id]
    elif args.airing:
        chosen = [e['id'] for e in index if (e.get('status') or '').upper() == 'RELEASING']
    else:
        ranked = sorted(index, key=lambda e: -(e.get('popularity') or 0))
        chosen = [e['id'] for e in ranked[:args.top]]
    return [(i, os.path.join(repo, 'data', 'anime', str(i // 1000), str(i), 'index.json'))
            for i in chosen]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--repo', required=True)
    ap.add_argument('--top', type=int, default=10, help='crawl the N most popular titles')
    ap.add_argument('--anilist', type=int, nargs='*', help='crawl these AniList ids instead')
    ap.add_argument('--airing', action='store_true', help='crawl everything still releasing')
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--refresh', action='store_true',
                    help='re-resolve every episode instead of carrying a stored record forward; '
                         'what the year rollover needs, when staged paths move into the archive')
    ap.add_argument('--skip-fresh-days', type=int, default=0,
                    help='leave an archive entry alone if it was crawled within N days')
    args = ap.parse_args()

    targets = select(args.repo, args)
    print(f'{len(targets)} titles selected', file=sys.stderr)

    episodes_total = missing = skipped = 0
    for anilist_id, index_path in targets:
        if not os.path.exists(index_path):
            print(f'{anilist_id}: not in the tree')
            continue
        title = read_json(index_path)
        label = (title.get('titles') or {}).get('english') or (title.get('titles') or {}).get('romaji') or ''
        if not title.get('bangumi'):
            print(f'{anilist_id} {label[:34]:<34} unmapped')
            missing += 1
            continue

        out_path = os.path.join(os.path.dirname(index_path), 'lili.json')
        prior = read_json(out_path) if os.path.exists(out_path) else None
        if args.skip_fresh_days and prior:
            age = (time.time() - time.mktime(time.strptime(prior['crawled'], '%Y-%m-%d'))) / 86400
            # An archive path has not moved in years; only `temp` is worth re-checking.
            if prior.get('layout') == 'archive' and age < args.skip_fresh_days:
                skipped += 1
                continue

        try:
            record = crawl(title, args.concurrency, None if args.refresh else prior)
        except Exception as exc:
            print(f'{anilist_id} {label[:34]:<34} FAILED {type(exc).__name__}: {exc}')
            continue
        if not record:
            print(f'{anilist_id} {label[:34]:<34} not on the site')
            missing += 1
            continue

        episodes_total += len(record['episodes'])
        print(f'{anilist_id} {label[:34]:<34} {record["layout"]:<8} '
              f'{len(record["episodes"]):>4} eps  {record["subject"]}')
        if args.write:
            with open(out_path, 'w', encoding='utf-8') as fh:
                json.dump(record, fh, ensure_ascii=False, separators=(',', ':'))

    print(f'\n{episodes_total} episodes, {missing} titles unavailable, {skipped} still fresh')
    if not args.write:
        print('(dry run -- nothing written; pass --write)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
