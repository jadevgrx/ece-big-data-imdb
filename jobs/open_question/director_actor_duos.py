"""Open question: do directors make better movies with actors they know?

For every director, his feature-length movies are split in two groups:
"reunion" movies, where at least one credited actor or actress had already
worked with him on --min-previous earlier movies, and "new cast" movies,
where nobody had. Each director is compared with himself (paired
comparison), so the result does not depend on how good a director is in
general.

Definitions (all thresholds are parameters):
- movies: titleType "movie", runtime of at least 40 minutes, released (not
  flagged is_future_release), with at least --min-votes votes;
- directors come from title_crew, actors from the actor and actress
  categories of title_principals (main credits only). A director acting in
  his own movie is not his own collaborator;
- "earlier" means a strictly earlier release year, so two movies of the
  same year never count for each other. Only past movies are used: a pair
  that worked again because their first movie was a success must not make
  that first movie a reunion;
- reunions are impossible in a first movie, and directors usually improve
  or decline with time, so the first --skip-first movies of every director
  are left out of both groups to compare movies from a similar stage of
  the career;
- a director is compared only if he has at least --min-each movies in each
  group.

Outputs (CSV in data/open_question/director_actor_duos/<settings>):
- paired_summary: mean and median difference of rating, share of directors
  rated higher with a reunion cast, t statistic of the mean difference;
- by_returning_actors: average rating gap to the director's own mean,
  by number of returning actors in the movie (0, 1, 2, 3 or more);
- directors: the per director comparison;
- top_duos: the director and actor pairs with the most movies together,
  with the rating of their shared movies against the director's others.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g \\
        /home/jovyan/jobs/open_question/director_actor_duos.py
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MIN_RUNTIME = 40
ACTOR_CATEGORIES = ["actor", "actress"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("director_actor_duos")


def rated_movies(titles, ratings, min_votes):
    """Released feature-length movies with enough votes."""
    return (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
            & ~F.col("is_future_release")
            & F.col("startYear").isNotNull()
        )
        .join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
        .select("tconst", "primaryTitle", "startYear", "averageRating",
                "numVotes")
    )


def director_movies(movies, crew):
    """One row per (director, movie)."""
    return (
        crew.select("tconst", F.explode("directors").alias("director"))
        .join(movies, "tconst")
        .distinct()
    )


def actor_credits(movies, principals):
    """One row per (actor, movie), main credits only."""
    return (
        principals
        .filter(F.col("category").isin(ACTOR_CATEGORIES))
        .select("tconst", F.col("nconst").alias("actor"))
        .join(movies.select("tconst"), "tconst", "left_semi")
        .distinct()
    )


def pair_credits(directed, actors):
    """(director, actor, movie) with the number of earlier movies together.

    The window counts, for each pair, the movies released in a strictly
    earlier year than the current one.
    """
    pairs = (
        directed.select("director", "tconst", "startYear")
        .join(actors, "tconst")
        .filter(F.col("director") != F.col("actor"))
    )
    earlier = (
        Window.partitionBy("director", "actor")
        .orderBy("startYear")
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    return pairs.withColumn("previous_together",
                            F.count("*").over(earlier))


def movie_groups(directed, pairs, min_previous, skip_first):
    """Label every (director, movie) as reunion or new cast.

    Movies without any credited actor (often documentaries) are left out,
    since "new cast" would mean nothing for them.
    """
    per_movie = pairs.groupBy("director", "tconst").agg(
        F.count("*").alias("n_actors"),
        F.sum(F.when(F.col("previous_together") >= min_previous, 1)
              .otherwise(0)).alias("n_returning"),
    )
    position = Window.partitionBy("director").orderBy("startYear", "tconst")
    return (
        directed
        .withColumn("career_position", F.row_number().over(position))
        .join(per_movie, ["director", "tconst"])
        .filter(F.col("career_position") > skip_first)
        .withColumn("is_reunion", F.col("n_returning") > 0)
    )


def paired_comparison(groups, min_each):
    """Per director: mean rating of reunion movies minus new cast ones."""
    reunion = F.col("is_reunion")
    return (
        groups.groupBy("director")
        .agg(
            F.sum(reunion.cast("int")).alias("n_reunion"),
            F.sum((~reunion).cast("int")).alias("n_new_cast"),
            F.avg(F.when(reunion, F.col("averageRating")))
            .alias("reunion_avg"),
            F.avg(F.when(~reunion, F.col("averageRating")))
            .alias("new_cast_avg"),
            F.expr("percentile_approx(CASE WHEN is_reunion THEN numVotes "
                   "END, 0.5)").alias("reunion_median_votes"),
            F.expr("percentile_approx(CASE WHEN NOT is_reunion THEN "
                   "numVotes END, 0.5)").alias("new_cast_median_votes"),
        )
        .filter((F.col("n_reunion") >= min_each)
                & (F.col("n_new_cast") >= min_each))
        .withColumn("diff", F.col("reunion_avg") - F.col("new_cast_avg"))
    )


def paired_summary(paired):
    """Mean, median, share of positive differences and t statistic."""
    return (
        paired.agg(
            F.count("*").alias("n_directors"),
            F.avg("diff").alias("mean_diff"),
            F.percentile_approx("diff", 0.5).alias("median_diff"),
            F.stddev("diff").alias("std_diff"),
            (100.0 * F.avg((F.col("diff") > 0).cast("int")))
            .alias("pct_directors_higher_with_reunion"),
            F.avg(F.log10("reunion_median_votes")
                  - F.log10("new_cast_median_votes"))
            .alias("mean_log10_votes_ratio"),
        )
        .withColumn("t_stat", F.col("mean_diff")
                    / (F.col("std_diff") / F.sqrt("n_directors")))
        .select(
            "n_directors",
            F.round("mean_diff", 3).alias("mean_diff"),
            F.round("median_diff", 3).alias("median_diff"),
            F.round("std_diff", 3).alias("std_diff"),
            F.round("t_stat", 2).alias("t_stat"),
            F.round("pct_directors_higher_with_reunion", 1)
            .alias("pct_directors_higher_with_reunion"),
            F.round("mean_log10_votes_ratio", 3)
            .alias("mean_log10_votes_ratio"),
        )
    )


def by_returning_actors(groups, paired):
    """Gap to the director's own mean by number of returning actors."""
    compared = groups.join(paired.select("director"), "director",
                           "left_semi")
    own_mean = Window.partitionBy("director")
    bucket = F.when(F.col("n_returning") >= 3, "3+").otherwise(
        F.col("n_returning").cast("string"))
    return (
        compared
        .withColumn("gap", F.col("averageRating")
                    - F.avg("averageRating").over(own_mean))
        .groupBy(bucket.alias("returning_actors"))
        .agg(
            F.count("*").alias("n_movies"),
            F.round(F.avg("gap"), 3).alias("avg_gap_to_director_mean"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
        )
        .orderBy("returning_actors")
    )


def top_duos(directed, pairs, min_duo_movies):
    """Pairs with many movies together: their rating against the rest."""
    together = (
        pairs.groupBy("director", "actor")
        .agg(F.collect_set("tconst").alias("shared"))
        .filter(F.size("shared") >= min_duo_movies)
    )
    rows = directed.join(together, "director").withColumn(
        "is_shared", F.array_contains("shared", F.col("tconst")))
    return (
        rows.groupBy("director", "actor")
        .agg(
            F.sum(F.col("is_shared").cast("int")).alias("n_together"),
            F.sum((~F.col("is_shared")).cast("int")).alias("n_without"),
            F.avg(F.when(F.col("is_shared"), F.col("averageRating")))
            .alias("together_avg"),
            F.avg(F.when(~F.col("is_shared"), F.col("averageRating")))
            .alias("without_avg"),
        )
        .filter(F.col("n_without") > 0)
        .withColumn("diff", F.col("together_avg") - F.col("without_avg"))
    )


def save_csv(df, path):
    """Write a small result as a single CSV file with a header."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(path))


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default="/home/jovyan/data")
    parser.add_argument("--min-votes", type=int, default=1000,
                        help="Minimum votes of a movie (default: %(default)s)")
    parser.add_argument("--min-previous", type=int, default=1,
                        help="Earlier movies together for an actor to count "
                             "as returning (default: %(default)s)")
    parser.add_argument("--skip-first", type=int, default=2,
                        help="First movies of each director left out "
                             "(default: %(default)s)")
    parser.add_argument("--min-each", type=int, default=2,
                        help="Minimum movies in each group per director "
                             "(default: %(default)s)")
    parser.add_argument("--min-duo-movies", type=int, default=4,
                        help="Minimum movies together for the top duos "
                             "(default: %(default)s)")
    return parser.parse_args()


def main():
    """Run the analysis, print the tables and save them as CSV."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    settings = (f"votes_{args.min_votes}_prev_{args.min_previous}"
                f"_skip_{args.skip_first}_each_{args.min_each}")
    output_dir = (data_dir / "open_question" / "director_actor_duos"
                  / settings)

    spark = (
        SparkSession.builder
        .appName("director_actor_duos")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read(name):
        return spark.read.parquet(str(clean_dir / name))

    movies = rated_movies(read("title_basics"), read("title_ratings"),
                          args.min_votes).cache()
    directed = director_movies(movies, read("title_crew")).cache()
    actors = actor_credits(movies, read("title_principals"))
    pairs = pair_credits(directed, actors).cache()
    groups = movie_groups(directed, pairs, args.min_previous,
                          args.skip_first).cache()
    paired = paired_comparison(groups, args.min_each).cache()
    names = read("name_basics").select("nconst", "primaryName")

    log.info("%d movies, %d (director, movie) rows, %d directors compared",
             movies.count(), directed.count(), paired.count())

    counts = groups.groupBy("is_reunion").count().orderBy("is_reunion")
    print("\n=== Movies by group (after leaving out first movies) ===")
    counts.show()

    summary = paired_summary(paired)
    print("=== Paired comparison: reunion cast minus new cast ===")
    summary.show(truncate=False)

    dose = by_returning_actors(groups, paired)
    print("=== Gap to the director's own mean by returning actors ===")
    dose.show(truncate=False)

    directors = (
        paired.join(names.withColumnRenamed("nconst", "director"),
                    "director")
        .select(
            "director", "primaryName", "n_reunion", "n_new_cast",
            F.round("reunion_avg", 2).alias("reunion_avg"),
            F.round("new_cast_avg", 2).alias("new_cast_avg"),
            F.round("diff", 2).alias("diff"),
        )
        .orderBy(F.desc(F.col("n_reunion") + F.col("n_new_cast")))
    )
    print("=== Directors with the most compared movies ===")
    directors.show(20, truncate=False)

    duos = (
        top_duos(directed, pairs, args.min_duo_movies)
        .join(names.select(F.col("nconst").alias("director"),
                           F.col("primaryName").alias("director_name")),
              "director")
        .join(names.select(F.col("nconst").alias("actor"),
                           F.col("primaryName").alias("actor_name")),
              "actor")
        .select(
            "director_name", "actor_name", "n_together", "n_without",
            F.round("together_avg", 2).alias("together_avg"),
            F.round("without_avg", 2).alias("without_avg"),
            F.round("diff", 2).alias("diff"),
        )
        .orderBy(F.desc("n_together"), F.desc("diff"))
    )
    print("=== Director and actor pairs with the most movies together ===")
    duos.show(25, truncate=False)

    save_csv(counts, output_dir / "group_counts")
    save_csv(summary, output_dir / "paired_summary")
    save_csv(dose, output_dir / "by_returning_actors")
    save_csv(directors, output_dir / "directors")
    save_csv(duos, output_dir / "top_duos")
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
