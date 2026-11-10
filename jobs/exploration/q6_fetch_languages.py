"""Q6: get the original language(s) of the successful movies from Wikidata.

Reuses the Wikidata mapping phase of jobs/enrichment/wiki_scraper.py
(property P364, batches of 200 IDs, rate limited and resumable). Each
record keeps the Wikidata URI as its source.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    python -u jobs/exploration/q6_fetch_languages.py --contact you@x.fr
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "enrichment"))

from wiki_scraper import PoliteClient, load_ids, phase_map  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--q6-dir", default="data/exploration/q6")
    parser.add_argument("--contact", required=True,
                        help="email shown in the User-Agent")
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()

    os.makedirs(args.q6_dir, exist_ok=True)
    ids = load_ids(f"{args.q6_dir}/ids_movies/*.csv", "tconst")
    client = PoliteClient(args.contact, args.delay)
    phase_map(client, ids, f"{args.q6_dir}/languages.jsonl", batch_size=200)


if __name__ == "__main__":
    main()