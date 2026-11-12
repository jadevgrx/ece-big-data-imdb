# ECE Big Data Processing: IMDb project

Group gr-01. This file explains how to set up the environment and run every
part of the project from scratch. Results and methodology are in
[REPORT.md](REPORT.md).

## Contributors

| Name | GitHub | Email | Main parts |
| --- | --- | --- | --- |
| Jade Vigouroux | jadevgrx | jade.vigou@gmail.com | Infrastructure, IMDb ingestion, streaming, PROJECT.md |
| Adam | adamkh92 | adam05khe@gmail.com | Data preparation, exploration Q1 to Q4 and Q8 |
| Melchior | MELKKIORR | melchior.gerrier13@gmail.com | Exploration Q5 to Q7, data enrichment |

## Prerequisites

- Docker Desktop (on Windows, with the WSL 2 backend). Give Docker at least
  6 GB of memory, 8 GB is better.
- Git.
- About 10 GB of free disk space: the IMDb files take about 2 GB compressed,
  plus the Parquet copies and the outputs.
- An internet connection: the IMDb files, the Spark Kafka connector, the
  Wikimedia stream and the Wikipedia/Wikidata APIs are downloaded at run time.

Everything runs inside Docker containers, so no Python, Java or Spark
installation is needed on the host.

### Note for Windows users

The commands below can be run in PowerShell or in Git Bash. In Git Bash,
paths starting with `/` are converted to Windows paths before being passed to
Docker, which breaks commands such as `docker exec ... /home/jovyan/...`.
Disable this conversion once:

```bash
echo 'export MSYS_NO_PATHCONV=1' >> ~/.bashrc
source ~/.bashrc
```

## 1. Start the environment

```bash
git clone https://github.com/jadevgrx/ece-big-data-imdb.git
cd ece-big-data-imdb
docker compose up -d
docker ps
```

`docker ps` must show two running containers, `kafka` and
`pyspark_notebook`.

The stack is defined in `compose.yaml`:

- `kafka`: Apache Kafka 4.1.1 in KRaft mode (single node). Other containers
  reach it at `kafka:19092`, the host at `localhost:9092`.
- `pyspark_notebook`: Jupyter with Spark 4.2.0 (Scala 2.13). The image is
  pinned by digest so that everybody runs the same versions. The folders
  `jobs/`, `data/` and `notebooks/` of the repository are mounted in
  `/home/jovyan/`.
- `producer`: the Wikipedia to Kafka producer. It only starts when called
  explicitly (see section 6).

If you followed the course lab `05.structured-streaming`, its containers use
the same names. Remove them first, otherwise `docker compose up` fails:

```bash
docker rm -f kafka pyspark_notebook
```

### Things to know about this environment

- Spark scripts must be run with `spark-submit`. A plain `python` started
  with `docker exec` does not see PySpark in this image.
- Spark 4 runs in ANSI mode by default: casting the IMDb null marker `\N` to a
  number raises an error instead of returning null. All our jobs use
  `try_cast` or replace `\N` with null before casting.
- All timestamps written by the jobs are in UTC.

## 2. Ingest the IMDb dataset

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/ingest_imdb.py
```

The script downloads the 7 files from <https://datasets.imdbws.com/> into
`data/imdb/tsv/` and converts each one to Parquet in `data/imdb/raw/`. It
takes 15 to 40 minutes. A file that is already downloaded is not downloaded
again; use `--force` to download it again, and `--tables` to process only
some tables, for example `--tables title.ratings`.

The raw layer keeps every column as a string and keeps `\N` as it is, so that
the preparation step measures null values on the original data. IMDb updates
its files every day, so results can differ slightly depending on the download
date.

## 3. Data preparation and exploration Q1 to Q4 and Q8

<!-- TODO Adam: list your scripts in the order they must run, with the exact
command for each one and what they write. -->

## 4. Exploration Q5 to Q7

Q5, series whose worst episode is in the last season and best episode in the
first two seasons (all series, then ended series only):

```bash
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q5_series.py
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q5_series.py --ended-only --out-dir data/exploration/q5_ended
```

Q6, actors and actresses successful in films of several languages. The
original language of each film comes from Wikidata, so the second step calls
its API (replace the email address with yours, it is sent in the User-Agent
as Wikimedia asks):

```bash
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q6_languages.py --step export
docker exec -w /home/jovyan pyspark_notebook python -u jobs/exploration/q6_fetch_languages.py --contact you@example.com
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q6_languages.py --step analyze
```

<!-- TODO Melchior: add the Q7 command. -->

## 5. Data enrichment

The three steps must run in this order.

Select the top 10 films per genre and year (1980 to 2025) and their first 3
principals:

```bash
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/enrichment/select_films.py
```

Get the Wikipedia articles through Wikidata. The script respects the
Wikimedia rate limits and can be stopped and restarted: it resumes where it
stopped. The full run takes 2 to 3 hours, so it is started in the background
with its log in `data/enrichment/scraper.log`:

```bash
docker exec -d -w /home/jovyan pyspark_notebook bash -c "python -u jobs/enrichment/wiki_scraper.py --contact you@example.com > data/enrichment/scraper.log 2>&1"
```

To test on a few films first, run the same script in the foreground with
`--limit 20`.

Clean the texts, extract the synopses and join them to the selection with
Spark:

```bash
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/enrichment/clean_enrichment.py
```

## 6. Streaming

Build the IMDb lookup table used to recognize Wikipedia pages (once, after
the ingestion):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/wiki_imdb_lookup.py
```

Then use two terminals. In the first one, start the Spark Structured
Streaming job (here for 30 minutes):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /home/jovyan/jobs/stream_wiki_imdb.py --minutes 30 --starting-offsets latest --report-every 30
```

When the first `batch=` lines are printed, start the producer in the second
terminal. It reads the Wikimedia recent changes stream and publishes the
article edits of the English, French, German, Spanish and Italian Wikipedia
to the Kafka topic `wikistreams`:

```bash
docker compose run --rm --build producer --minutes 32
```

When the job stops, print a summary of the outputs and metrics:

```bash
docker exec pyspark_notebook spark-submit /home/jovyan/jobs/show_stream_results.py
```

The outputs are written in `data/streaming/`: `matched_events`,
`edits_per_window` and `trending_entities` as Parquet, and the progress of
every micro-batch in `progress.jsonl`. Spark keeps its checkpoints in
`data/streaming/checkpoints/`, so a new run resumes from the last offsets. To
start a test from zero, delete the folder first:

```bash
docker exec pyspark_notebook rm -rf /home/jovyan/data/streaming
```

## 7. Open question

<!-- TODO: commands of the open question. -->

## Stopping the environment

```bash
docker compose down
```

The data stays in `data/` on the host. Run `docker compose up -d` to start
again.

## Use of AI tools

All three of us used Claude (Anthropic) as a coding assistant: writing and
debugging the Python and PySpark code, the Docker configuration and the
setup commands, and drafting this PROJECT.md. AI was not used for the
explanations and reasoning in REPORT.md, which we wrote ourselves.

| Member | Tool | Tasks |
| --- | --- | --- |
| Jade Vigouroux | Claude | Docker Compose stack, IMDb ingestion, Wikipedia producer, streaming jobs, setup commands, PROJECT.md |
| Adam | Claude | Code of the data preparation and of exploration Q1 to Q4 and Q8 |
| Melchior | Claude | Code of exploration Q5 to Q7 and of the enrichment (selection, scraper, cleaning) |
