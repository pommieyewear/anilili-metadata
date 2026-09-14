#!/usr/bin/env python3
"""Join the anilili metadata tree to Bangumi subject ids, offline.

Bangumi is what makes the Chinese source catalogue reachable: those sites index by Chinese
name, and the name they use is often an *alias* rather than Bangumi's own `name_cn`
(嘀嗒影视 lists 躲在超市后门抽烟的两人 for a subject whose name_cn is 在超市后门吸烟的二人). Reading
aliases costs a second API call per candidate, so this runs against the weekly Archive dump
instead of the search API -- which also matches better, because it sees every name in the
catalogue at once rather than the top few rows of a search ranking.

    python bangumi_backfill.py --dump dump.zip --repo path/to/anilili-metadata [--write]

Without --write nothing is modified; the summary and the review file are still produced.
"""
from __future__ import annotations

import argparse, json, os, re, sys, unicodedata, zipfile
from collections import Counter

# --- Bangumi infobox is wikitext, not JSON -----------------------------------------------

ALIAS_BLOCK = re.compile(r'\|\s*别名\s*=\s*\{(.*?)\}', re.S)
ALIAS_ITEM = re.compile(r'^\s*\[(.+?)\]\s*$', re.M)
EPS_FIELD = re.compile(r'\|\s*话数\s*=\s*([^\r\n|]*)')
AIR_FIELD = re.compile(r'\|\s*放送开始\s*=\s*([^\r\n|]*)')

SUBJECT_TYPE_ANIME = 2


def parse_infobox(raw: str | None) -> tuple[list[str], int | None, str | None]:
    aliases: list[str] = []
    eps = air = None
    if not raw:
        return aliases, eps, air
    block = ALIAS_BLOCK.search(raw)
    if block:
        aliases = [a.strip() for a in ALIAS_ITEM.findall(block.group(1)) if a.strip()]
    m = EPS_FIELD.search(raw)
    if m:
        digits = re.sub(r'\D', '', m.group(1))
        eps = int(digits) if digits else None
    m = AIR_FIELD.search(raw)
    if m:
        air = m.group(1).strip() or None
    return aliases, eps, air


def load_subjects(dump_path: str) -> dict[int, dict]:
    subjects: dict[int, dict] = {}
    with zipfile.ZipFile(dump_path) as z, z.open('subject.jsonlines') as fh:
        for line in fh:
            row = json.loads(line)
            if row.get('type') != SUBJECT_TYPE_ANIME:
                continue
            aliases, eps, air = parse_infobox(row.get('infobox'))
            subjects[row['id']] = {
                'name': row.get('name') or '',
                'name_cn': row.get('name_cn') or '',
                'aliases': aliases,
                'date': row.get('date') or air,
                'eps': eps,
                'nsfw': bool(row.get('nsfw')),
            }
    return subjects


# --- matching ----------------------------------------------------------------------------

_PUNCT = " 　·・:：!！?？,，.。-–—_~〜'\"“”‘’()（）[]【】/\\\t"
PUNCT_RE = re.compile('[' + re.escape(_PUNCT) + ']+')


def norm(s: str | None) -> str:
    if not s:
        return ''
    return PUNCT_RE.sub('', unicodedata.normalize('NFKC', s).lower())


def build_name_index(subjects: dict[int, dict]) -> dict[str, set[int]]:
    index: dict[str, set[int]] = {}
    for sid, rec in subjects.items():
        for name in (rec['name'], rec['name_cn'], *rec['aliases']):
            key = norm(name)
            if key:
                index.setdefault(key, set()).add(sid)
    return index


def year_of(date: str | None) -> int | None:
    m = re.match(r'(\d{4})', date or '')
    return int(m.group(1)) if m else None


def match(title: dict, subjects: dict[int, dict], index: dict[str, set[int]]):
    """Return (bangumi_id, confidence). Confidence is never a guess about correctness --
    STRONG means air year and episode count both corroborate the name hit."""
    hits: set[int] = set()
    for field in ('native', 'romaji', 'english'):
        key = norm(title.get(field))
        if key:
            hits |= index.get(key, set())
    if not hits:
        return None, 'NONE'

    native = norm(title.get('native'))
    ranked = []
    for sid in hits:
        rec = subjects[sid]
        by, ay = year_of(rec['date']), title.get('year')
        dy = abs(by - ay) if (by and ay) else None
        eps_ok = (not title.get('episodes') or not rec['eps']
                  or abs(rec['eps'] - title['episodes']) <= 2)
        pool = {norm(rec['name']), norm(rec['name_cn'])} | {norm(a) for a in rec['aliases']}
        ranked.append((
            0 if (dy is not None and dy <= 1) else 1,
            0 if (native and native in pool) else 1,
            0 if eps_ok else 1,
            dy if dy is not None else 99,
            sid,
        ))
    ranked.sort()
    year_ok, _, eps_flag, _, sid = ranked[0]
    if year_ok == 0 and eps_flag == 0:
        conf = 'STRONG'
    elif year_ok == 0:
        conf = 'WEAK'
    else:
        conf = 'REVIEW'
    return sid, conf


# --- repo walk ---------------------------------------------------------------------------

def title_files(repo: str):
    root = os.path.join(repo, 'data', 'anime')
    for shard in sorted(os.listdir(root)):
        shard_dir = os.path.join(root, shard)
        if not os.path.isdir(shard_dir):
            continue
        for aid in sorted(os.listdir(shard_dir)):
            path = os.path.join(shard_dir, aid, 'index.json')
            if os.path.exists(path):
                yield int(aid), path


def read_json(path):
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


def write_json(path, obj):
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, ensure_ascii=False, separators=(',', ':'))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dump', required=True)
    ap.add_argument('--repo', required=True)
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--overrides', default=None,
                    help='JSON {anilist_id: bangumi_id}; wins over any automatic match')
    ap.add_argument('--bangumi-data', default=None,
                    help="bangumi-data dist/data.json; its curated aniList<->bangumi pairs seed "
                         "titles the name matcher could not place")
    args = ap.parse_args()

    overrides = {}
    ov_path = args.overrides or os.path.join(args.repo, 'data', 'bangumi-overrides.json')
    if os.path.exists(ov_path):
        # Keys opening with '_' are the file's own prose (`_comment`, `_notes`), not mappings.
        overrides = {int(k): v for k, v in read_json(ov_path).items()
                     if not k.startswith('_')}
    print(f'overrides loaded: {len(overrides)}', file=sys.stderr)

    seed = {}
    if args.bangumi_data and os.path.exists(args.bangumi_data):
        for item in read_json(args.bangumi_data)['items']:
            sites = {s['site']: s.get('id') for s in item.get('sites', []) if s.get('id')}
            if 'bangumi' in sites and 'aniList' in sites:
                try:
                    seed[int(sites['aniList'])] = int(sites['bangumi'])
                except ValueError:
                    pass
    print(f'bangumi-data seed pairs: {len(seed):,}', file=sys.stderr)

    subjects = load_subjects(args.dump)
    index = build_name_index(subjects)
    print(f'dump: {len(subjects):,} anime subjects, {len(index):,} distinct names', file=sys.stderr)

    id_map_path = os.path.join(args.repo, 'data', 'id-map.json')
    id_map = read_json(id_map_path)

    counts = Counter()
    review = []
    for anilist_id, path in title_files(args.repo):
        rec = read_json(path)
        titles = rec.get('titles') or {}
        probe = {
            'native': titles.get('native'), 'romaji': titles.get('romaji'),
            'english': titles.get('english'), 'year': rec.get('season_year'),
            'episodes': rec.get('episodes'),
        }
        # Precedence: a hand override, then whatever the name matcher is confident about, then
        # bangumi-data. Scored against bangumi-data's 7,306 overlapping curated pairs the matcher
        # agrees 99.0%, and the 76 it does not are franchise granularity (ONE PIECE the series
        # versus ONE PIECE エルバフ編, SEED versus the HD remaster) where the corroborated match is
        # the better answer -- so a STRONG hit is kept and the conflict is queued, not overwritten.
        if anilist_id in overrides:
            sid, conf = overrides[anilist_id], 'OVERRIDE'
        else:
            sid, conf = match(probe, subjects, index)
            seeded = seed.get(anilist_id)
            if seeded and conf == 'STRONG' and seeded != sid:
                review.append({'anilist': anilist_id, 'reason': 'CONFLICT',
                               'bangumi': sid, 'bangumi_data': seeded,
                               'titles': {k: titles.get(k) for k in ('native', 'romaji', 'english')}})
                counts['CONFLICT'] += 1
            elif seeded and conf != 'STRONG':
                sid, conf = seeded, 'SEED'
        counts[conf] += 1

        if sid is None:
            review.append({'anilist': anilist_id, 'reason': 'NONE',
                           'titles': {k: titles.get(k) for k in ('native', 'romaji', 'english')},
                           'year': rec.get('season_year')})
            continue
        if conf in ('WEAK', 'REVIEW'):
            sub = subjects.get(sid, {})
            review.append({'anilist': anilist_id, 'reason': conf, 'bangumi': sid,
                           'bangumi_name': sub.get('name'), 'bangumi_date': sub.get('date'),
                           'titles': {k: titles.get(k) for k in ('native', 'romaji', 'english')},
                           'year': rec.get('season_year')})

        sub = subjects.get(sid)
        if sub and args.write:
            # Search keys for the Chinese source catalogue. Dedupe but keep order: name_cn
            # first, because that is what most sites title a page with.
            seen, keys = set(), []
            for n in (sub['name_cn'], sub['name'], *sub['aliases']):
                if n and n not in seen:
                    seen.add(n)
                    keys.append(n)
            rec['bangumi'] = {'id': sid, 'name': sub['name'], 'name_cn': sub['name_cn'],
                              'search_names': keys, 'confidence': conf}
            write_json(path, rec)

        entry = id_map.setdefault(str(anilist_id), {})
        if args.write:
            entry['bangumi'] = sid
            entry['bangumi_confidence'] = conf

    if args.write:
        write_json(id_map_path, id_map)
        write_json(os.path.join(args.repo, 'data', 'bangumi-review.json'), review)

    # CONFLICT is a tally of queued disagreements, not an outcome, so it is not part of the total.
    total = sum(v for k, v in counts.items() if k != 'CONFLICT')
    matched = total - counts['NONE']
    print(f'\ntitles: {total:,}')
    for k in ('OVERRIDE', 'STRONG', 'SEED', 'WEAK', 'REVIEW', 'NONE', 'CONFLICT'):
        if counts[k]:
            print(f'  {k:<9} {counts[k]:>6,}  ({100*counts[k]/total:.1f}%)')
    print(f'matched: {matched:,}/{total:,} ({100*matched/total:.1f}%)  review queue: {len(review):,}')
    if not args.write:
        print('\n(dry run -- nothing written; pass --write)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
