# ECE Big Data Processing: IMDb project

Group gr-01. This file explains how to set up the environment and run every
part of the project from scratch. Results and methodology are in
[REPORT.md](REPORT.md).

## Contributors

| Name | GitHub | Email | Main parts |
| --- | --- | --- | --- |
| Jade Vigouroux | jadevgrx | jade.vigou@gmail.com | Infrastructure, IMDb ingestion, streaming, PROJECT.md |
| Adam Kheloufi | adamkh92 | adam05khe@gmail.com | Data preparation, exploration Q1 to Q4 and Q8 |
| Melchior Gerrier | MELKKIORR | melchior.gerrier13@gmail.com | Exploration Q5 to Q7, data enrichment |

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
date. Each run records in `data/imdb/manifest.json`, for every table, the
source URL, the download time, the Last-Modified date sent by IMDb and the
number of rows. A table with 0 rows (for instance after a corrupted download)
stops the script with the command to download it again.

## 3. Data preparation

The three scripts read the raw layer. The first two only measure and
report; the third one builds the clean layer used by Q1 to Q4 and Q8.

Percentage of null, missing and empty values for every column of every
table (`--sample 0.01` gives a quick approximate run):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/preparation/missing_values.py
```

Data quality checks (impossible dates, orphan foreign keys, duplicates,
out of range values). It only counts and shows examples, it changes
nothing. A single check can be run with `--only`, for example
`--only death_before_birth`. The full run takes about 15 minutes:

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g --conf "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2g" /home/jovyan/jobs/preparation/quality_checks.py
```

Build the clean layer in `data/imdb/clean/` (typed columns, `\N` replaced
with null, lists as arrays, quality issues handled):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g --conf "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2g" /home/jovyan/jobs/preparation/clean_imdb.py
```

Outputs are written in `data/preparation/`.

## 4. Exploration Q1 to Q4 and Q8

These scripts read the clean layer, so section 3 must have run first.
Outputs are written in `data/exploration/`.

Q1, feature-length movies whose runtime is far from the average of their
genre (z-score by default, IQR as an alternative):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q1_runtime_outliers.py
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q1_runtime_outliers.py --method iqr
```

Q2 and Q3, top three genres per year from 1980 to 2025 and how they compare
with the other genres (popularity measured by votes by default, by number of
movies with `--metric movies`):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q2_q3_top_genres.py
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q2_q3_top_genres.py --metric movies
```

Q4, directors with exactly one movie rated 8 or more and a weak rest of
career:

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q4_one_hit_directors.py
```

Q8, multi-talented professionals and how they perform when they hold
several roles on the same movie (the script asks for 6 GB of driver
memory, so Docker needs at least 8 GB):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 6g /home/jovyan/jobs/exploration/q8_multi_talented.py
```

## 5. Exploration Q5 to Q7

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

Q7, actors and actresses with a career of 20 years or more and at least 15
movies whose best movies came early. The second run compares each rating
with the average of its year, to separate a career effect from the fact that
older movies are rated higher on IMDb:

```bash
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q7_early_peak.py
docker exec -w /home/jovyan pyspark_notebook spark-submit --driver-memory 4g jobs/exploration/q7_early_peak.py --adjust-year
```

Q5 to Q7 read the raw layer, so they only need section 2.

## 6. Data enrichment

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

## 7. Streaming

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
to the Kafka topic `wikistreams`. Events sent again after a reconnection are
dropped by their id:

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

### Late data test

To check the watermark, run the job with a separate output folder, then send
two test events for the page Inception from a second terminal: one dated now,
then 75 seconds later one dated 30 minutes earlier. The late one must be kept
in `matched_events` and dropped from the two aggregations (`late_dropped` in
the summary):

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /home/jovyan/jobs/stream_wiki_imdb.py --minutes 6 --starting-offsets latest --output-dir /home/jovyan/data/streaming_late_test --report-every 30
docker compose run --rm --build --entrypoint python producer inject_late_events.py --minutes-late 30
docker exec pyspark_notebook spark-submit /home/jovyan/jobs/show_stream_results.py --output-dir /home/jovyan/data/streaming_late_test
```

The first command goes in one terminal; start the second one when the job
prints `Lookup loaded`, and the third one when the job has stopped. The test
events are marked with the user `late-data-test`.

## 8. Open question

Do directors make better movies with actors they have already worked
with? The script reads the clean layer, so section 3 must have run first.
It uses no external data.

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/open_question/director_actor_duos.py
```

The two variants used in the report to check that the result does not
depend on the thresholds:

```bash
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/open_question/director_actor_duos.py --min-votes 10000
docker exec pyspark_notebook spark-submit --driver-memory 4g /home/jovyan/jobs/open_question/director_actor_duos.py --min-previous 2
```

Every setting writes its tables as CSV in its own folder,
`data/open_question/director_actor_duos/<settings>/`, so the runs do not
overwrite each other. The other thresholds are `--skip-first` (first
movies of each director left out, 2 by default), `--min-each` (minimum
movies per group and director, 2 by default) and `--min-duo-movies`
(minimum movies together in the table of duos, 4 by default).

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
