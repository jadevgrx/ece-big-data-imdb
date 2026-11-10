"""Collect raw Wikipedia/Wikidata data for IMDb IDs.

Data gathering is allowed outside Spark; cleaning happens in Spark later.
Three phases, each resumable (already collected IDs are skipped):

1. map    : IMDb ID -> Wikidata item, English Wikipedia article and
            original language(s), via SPARQL in batches.
2. films  : full plain text of each film article (plot extracted in Spark).
3. people : introduction of each person article, in batches of 20.

Usage (test on 20 films):
    python jobs/enrichment/wiki_scraper.py --contact you@edu.ece.fr --limit 20
"""

import argparse
import csv
import glob
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import unquote

import requests

SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://en.wikipedia.org/w/api.php"
ARTICLE_PREFIX = "https://en.wikipedia.org/wiki/"

SPARQL_TEMPLATE = """
SELECT ?imdb ?item ?article ?langLabel WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL { ?article schema:about ?item ;
                      schema:isPartOf <https://en.wikipedia.org/> . }
  OPTIONAL { ?item wdt:P364 ?lang .
             ?lang rdfs:label ?langLabel .
             FILTER(LANG(?langLabel) = "en") }
}
"""


def now():
    return datetime.now(timezone.utc).isoformat()


class PoliteClient:
    """HTTP client that identifies itself, waits and backs off."""

    def __init__(self, contact, delay):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            f"ECE-BigData-IMDb-Enrichment/1.0 ({contact})")
        self.delay = delay

    def request(self, method, url, max_attempts=6, **kwargs):
        for attempt in range(max_attempts):
            time.sleep(self.delay)
            try:
                resp = self.session.request(method, url, timeout=90, **kwargs)
            except requests.RequestException as exc:
                print(f"  network error ({exc}), retrying")
                time.sleep(5 * 2 ** attempt)
                continue
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = self._retry_after(resp, 5 * 2 ** attempt)
                print(f"  HTTP {resp.status_code}, waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            # MediaWiki asks clients to pause when replication lag is high.
            if isinstance(data, dict) and \
                    data.get("error", {}).get("code") == "maxlag":
                time.sleep(self._retry_after(resp, 5))
                continue
            return data
        raise RuntimeError(f"giving up on {url} after {max_attempts} tries")

    @staticmethod
    def _retry_after(resp, default):
        try:
            return int(resp.headers.get("Retry-After", default))
        except ValueError:
            return default


def load_ids(pattern, column):
    ids = []
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="", encoding="utf-8") as handle:
            ids.extend(row[column] for row in csv.DictReader(handle))
    return ids


def load_jsonl(path):
    """Read a JSONL file, skipping a line cut by an interrupted run."""
    if not os.path.exists(path):
        return []
    records = []
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"  skipping unreadable line {number} of {path}")
    return records


def append_jsonl(path, records):
    # If a previous run was killed mid-line, start on a fresh line.
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as handle:
            handle.seek(-1, os.SEEK_END)
            missing_newline = handle.read(1) != b"\n"
        if missing_newline:
            with open(path, "a", encoding="utf-8") as handle:
                handle.write("\n")
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def batches(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def phase_map(client, ids, out_path, batch_size):
    done = {r["imdb_id"] for r in load_jsonl(out_path)}
    todo = [i for i in ids if i not in done]
    print(f"[map] {len(todo)} IDs to map ({len(done)} already done)")
    for n, batch in enumerate(batches(todo, batch_size), start=1):
        values = " ".join(f'"{i}"' for i in batch)
        data = client.request(
            "POST", SPARQL_URL,
            data={"query": SPARQL_TEMPLATE % values},
            headers={"Accept": "application/sparql-results+json"},
        )
        found = {}
        for row in data["results"]["bindings"]:
            imdb_id = row["imdb"]["value"]
            entry = found.setdefault(imdb_id, {
                "wikidata_uri": row["item"]["value"],
                "article_url": row.get("article", {}).get("value"),
                "languages": set(),
            })
            if "langLabel" in row:
                entry["languages"].add(row["langLabel"]["value"])
        records = []
        for imdb_id in batch:
            entry = found.get(imdb_id)
            records.append({
                "imdb_id": imdb_id,
                "wikidata_uri": entry["wikidata_uri"] if entry else None,
                "article_url": entry["article_url"] if entry else None,
                "original_languages": sorted(entry["languages"])
                if entry else [],
                "status": "ok" if entry else "no_wikidata_item",
                "source_url": entry["wikidata_uri"] if entry else SPARQL_URL,
                "retrieved_at": now(),
            })
        append_jsonl(out_path, records)
        print(f"[map] batch {n}: {len(found)}/{len(batch)} found")


def title_from_url(url):
    return unquote(url[len(ARTICLE_PREFIX):]).replace("_", " ")


def fetch_extracts(client, titles, intro_only):
    """Return {requested title: (final title, text or None)}."""
    params = {
        "action": "query", "prop": "extracts", "explaintext": 1,
        "redirects": 1, "titles": "|".join(titles), "format": "json",
        "formatversion": 2, "maxlag": 5,
    }
    if intro_only:
        params.update({"exintro": 1, "exlimit": len(titles)})
    else:
        params["exsectionformat"] = "wiki"  # keeps "== Plot ==" markers

    pages, aliases = {}, {}
    while True:
        data = client.request("GET", API_URL, params=params)
        query = data.get("query", {})
        for key in ("normalized", "redirects"):
            for item in query.get(key, []):
                aliases[item["from"]] = item["to"]
        for page in query.get("pages", []):
            text = page.get("extract")
            if page["title"] not in pages or text:
                pages[page["title"]] = None if page.get("missing") else text
        if "continue" not in data:
            break
        params.update(data["continue"])

    result = {}
    for title in titles:
        final = title
        for _ in range(3):  # normalized -> redirect chains are short
            final = aliases.get(final, final)
        result[title] = (final, pages.get(final))
    return result


def phase_texts(client, mapping, ids, out_path, key, intro_only, batch_size):
    done = {r[key] for r in load_jsonl(out_path)}
    todo = [i for i in ids if i not in done]
    print(f"[{key}] {len(todo)} IDs to fetch ({len(done)} already done)")

    no_article = []
    with_article = []
    not_mapped = 0
    for imdb_id in todo:
        entry = mapping.get(imdb_id)
        if entry is None:
            # Not mapped yet (map phase unfinished or crashed): leave it
            # out so that the next run picks it up.
            not_mapped += 1
        elif entry["article_url"]:
            with_article.append(imdb_id)
        else:
            no_article.append({
                key: imdb_id,
                "wikidata_uri": entry["wikidata_uri"],
                "wikipedia_title": None,
                "text": None,
                "original_languages": entry["original_languages"],
                "status": entry["status"] if entry["status"] != "ok"
                else "no_enwiki_article",
                "source_url": entry["source_url"],
                "retrieved_at": now(),
            })
    if not_mapped:
        print(f"[{key}] {not_mapped} IDs skipped: not in the Wikidata "
              f"mapping yet, run the map phase first")
    append_jsonl(out_path, no_article)

    for n, batch in enumerate(batches(with_article, batch_size), start=1):
        titles = {i: title_from_url(mapping[i]["article_url"]) for i in batch}
        extracts = fetch_extracts(client, list(titles.values()), intro_only)
        records = []
        for imdb_id in batch:
            final, text = extracts[titles[imdb_id]]
            records.append({
                key: imdb_id,
                "wikidata_uri": mapping[imdb_id]["wikidata_uri"],
                "wikipedia_title": final,
                "text": text,
                "original_languages": mapping[imdb_id]["original_languages"],
                "status": "ok" if text else "empty_or_missing_page",
                "source_url": mapping[imdb_id]["article_url"],
                "retrieved_at": now(),
            })
        append_jsonl(out_path, records)
        if n % 25 == 0:
            print(f"[{key}] {n * batch_size} fetched")


def people_of_films(selection_dir, film_ids, people_ids, limit):
    """Principals of the given films, read from the selection Parquet.

    Used for test runs with --limit, so that the people fetched belong to
    the films fetched. Falls back to the first people of the list if
    pyarrow is not installed.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("pyarrow not found: --limit takes the first people instead")
        return people_ids[:3 * limit]
    pairs = pq.read_table(f"{selection_dir}/principals_top",
                          columns=["tconst", "nconst"]).to_pylist()
    wanted = set(film_ids)
    return sorted({p["nconst"] for p in pairs if p["tconst"] in wanted})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection-dir", default="data/enrichment/selection")
    parser.add_argument("--out-dir", default="data/enrichment/raw")
    parser.add_argument("--contact", required=True,
                        help="email shown in the User-Agent")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="seconds between two requests")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process the first N films (testing)")
    parser.add_argument("--phase", choices=["all", "map", "films", "people"],
                        default="all")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    film_ids = load_ids(f"{args.selection_dir}/ids_films/*.csv", "tconst")
    people_ids = load_ids(f"{args.selection_dir}/ids_people/*.csv", "nconst")
    if args.limit:
        film_ids = film_ids[:args.limit]
        people_ids = people_of_films(args.selection_dir, film_ids,
                                     people_ids, args.limit)

    client = PoliteClient(args.contact, args.delay)
    map_path = f"{args.out_dir}/wikidata_map.jsonl"

    if args.phase in ("all", "map"):
        phase_map(client, film_ids + people_ids, map_path, batch_size=200)

    mapping = {r["imdb_id"]: r for r in load_jsonl(map_path)}
    if args.phase in ("all", "films"):
        phase_texts(client, mapping, film_ids,
                    f"{args.out_dir}/films_raw.jsonl", "tconst",
                    intro_only=False, batch_size=1)
    if args.phase in ("all", "people"):
        phase_texts(client, mapping, people_ids,
                    f"{args.out_dir}/people_raw.jsonl", "nconst",
                    intro_only=True, batch_size=20)


if __name__ == "__main__":
    main()