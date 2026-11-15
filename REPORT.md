# ECE Big Data Processing: IMDb project report

Group gr-01. Setup and run instructions are in [PROJECT.md](PROJECT.md).

## 1. Data preparation

### 1.1 Ingestion

<!-- Jade -->

### 1.2 Null, missing and empty values

Missing values per column, all tables, computed on a 1% random sample of each table (`--sample 0.01`, seed 42). Null, "\N" marker and empty string are counted separately:

| table | column | total_rows | nulls | null_markers | empty_strings | missing | missing_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| title_akas | attributes | 604326 | 0 | 601230 | 0 | 601230 | 99.4877 |
| title_basics | endYear | 130019 | 0 | 128308 | 0 | 128308 | 98.684 |
| name_basics | deathYear | 158403 | 0 | 155686 | 0 | 155686 | 98.2848 |
| name_basics | birthYear | 158403 | 0 | 151457 | 0 | 151457 | 95.615 |
| title_principals | job | 1027845 | 0 | 829081 | 0 | 829081 | 80.6621 |
| title_akas | types | 604326 | 0 | 404188 | 0 | 404188 | 66.8824 |
| title_basics | runtimeMinutes | 130019 | 0 | 80816 | 0 | 80816 | 62.1571 |
| title_principals | characters | 1027845 | 0 | 527005 | 0 | 527005 | 51.2728 |
| title_crew | writers | 129683 | 0 | 64431 | 0 | 64431 | 49.6835 |
| title_crew | directors | 129683 | 0 | 57911 | 0 | 57911 | 44.6558 |
| title_akas | language | 604326 | 0 | 196868 | 0 | 196868 | 32.5765 |
| title_akas | region | 604326 | 0 | 130202 | 0 | 130202 | 21.545 |
| title_episode | seasonNumber | 100004 | 0 | 21225 | 0 | 21225 | 21.2242 |
| title_episode | episodeNumber | 100004 | 0 | 21225 | 0 | 21225 | 21.2242 |
| name_basics | primaryProfession | 158403 | 0 | 32197 | 0 | 32197 | 20.326 |
| name_basics | knownForTitles | 158403 | 0 | 19426 | 0 | 19426 | 12.2637 |
| title_basics | startYear | 130019 | 0 | 14898 | 0 | 14898 | 11.4583 |
| title_basics | genres | 130019 | 0 | 5434 | 0 | 5434 | 4.1794 |
| name_basics | primaryName | 158403 | 0 | 1 | 0 | 1 | 6.0E-4 |
| name_basics | nconst | 158403 | 0 | 0 | 0 | 0 | 0.0 |
| title_akas | titleId | 604326 | 0 | 0 | 0 | 0 | 0.0 |
| title_akas | ordering | 604326 | 0 | 0 | 0 | 0 | 0.0 |
| title_akas | title | 604326 | 0 | 0 | 0 | 0 | 0.0 |
| title_akas | isOriginalTitle | 604326 | 0 | 0 | 0 | 0 | 0.0 |
| title_basics | tconst | 130019 | 0 | 0 | 0 | 0 | 0.0 |
| title_basics | titleType | 130019 | 0 | 0 | 0 | 0 | 0.0 |
| title_basics | primaryTitle | 130019 | 0 | 0 | 0 | 0 | 0.0 |
| title_basics | originalTitle | 130019 | 0 | 0 | 0 | 0 | 0.0 |
| title_basics | isAdult | 130019 | 0 | 0 | 0 | 0 | 0.0 |
| title_crew | tconst | 129683 | 0 | 0 | 0 | 0 | 0.0 |
| title_episode | tconst | 100004 | 0 | 0 | 0 | 0 | 0.0 |
| title_episode | parentTconst | 100004 | 0 | 0 | 0 | 0 | 0.0 |
| title_principals | tconst | 1027845 | 0 | 0 | 0 | 0 | 0.0 |
| title_principals | ordering | 1027845 | 0 | 0 | 0 | 0 | 0.0 |
| title_principals | nconst | 1027845 | 0 | 0 | 0 | 0 | 0.0 |
| title_principals | category | 1027845 | 0 | 0 | 0 | 0 | 0.0 |
| title_ratings | tconst | 17482 | 0 | 0 | 0 | 0 | 0.0 |
| title_ratings | averageRating | 17482 | 0 | 0 | 0 | 0 | 0.0 |
| title_ratings | numVotes | 17482 | 0 | 0 | 0 | 0 | 0.0 |

Five columns with the highest share of missing values:

| table | column | total_rows | nulls | null_markers | empty_strings | missing | missing_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| title_akas | attributes | 604326 | 0 | 601230 | 0 | 601230 | 99.4877 |
| title_basics | endYear | 130019 | 0 | 128308 | 0 | 128308 | 98.684 |
| name_basics | deathYear | 158403 | 0 | 155686 | 0 | 155686 | 98.2848 |
| name_basics | birthYear | 158403 | 0 | 151457 | 0 | 151457 | 95.615 |
| title_principals | job | 1027845 | 0 | 829081 | 0 | 829081 | 80.6621 |

We count three kinds of missing value separately: real Spark nulls (`nulls`), the `\N` marker that IMDb uses in its TSV files (`null_markers`) and empty strings (`empty_strings`). `missing_pct` is the sum of the three as a share of the table. These figures come from a 1% sample of each table (seed 42), not from a run on the full data, so the percentages on the full tables may differ slightly. Every missing value we found is a `\N` marker. There are no real nulls and no empty strings in any table, which is expected since the raw layer keeps every column as text and leaves the marker as it is.

The five columns with the most missing values are attributes in title_akas (99.5%), endYear in title_basics (98.7%), deathYear (98.3%) and birthYear (95.6%) in name_basics, and job in title_principals (80.7%). Most of these are structural gaps rather than real ones. endYear only exists for TV series, deathYear is empty for people who are still alive, and job and attributes are optional fields that IMDb fills in only for some rows. birthYear is a different case: the person has a birth year in reality, IMDb just does not know it, so this is a real missing value.

Some of these rates matter for part 2. runtimeMinutes is missing for 62.2% of titles, which reduces the base for Q1. genres (4.2%) and startYear (11.5%) are needed for Q2 and Q3, so titles without them drop out of those analyses. The key columns (tconst, nconst, titleId) are never missing.

### 1.3 Other data quality issues

Data quality checks on the raw tables (`jobs/preparation/quality_checks.py`):

| check | table | total_rows | issue_rows | issue_pct |
| --- | --- | --- | --- | --- |
| orphan_principal_name | title_principals | 102649609 | 8441 | 0.0082 |
| start_in_future | title_basics | 12951113 | 6017 | 0.0465 |
| credit_before_birth | title_principals | 102649609 | 3089 | 0.003 |
| orphan_writer | title_crew | 12949786 | 1149 | 0.0089 |
| extreme_runtime | title_basics | 12951113 | 441 | 0.0034 |
| orphan_director | title_crew | 12949786 | 423 | 0.0033 |
| orphan_known_for_title | name_basics | 15802420 | 69 | 4.0E-4 |
| orphan_crew_title | title_crew | 12949786 | 30 | 2.0E-4 |
| death_before_birth | name_basics | 15802420 | 28 | 2.0E-4 |
| orphan_episode_parent | title_episode | 10025088 | 5 | 0.0 |
| non_positive_runtime | title_basics | 12951113 | 4 | 0.0 |
| implausible_lifespan | name_basics | 15802420 | 2 | 0.0 |
| orphan_aka_title | title_akas | 60321594 | 2 | 0.0 |
| duplicate_nconst | name_basics | 15802420 | 0 | 0.0 |
| died_in_future | name_basics | 15802420 | 0 | 0.0 |
| duplicate_principal_ordering | title_principals | 102649609 | 0 | 0.0 |
| invalid_is_adult | title_basics | 12951113 | 0 | 0.0 |
| rating_out_of_range | title_ratings | 1730885 | 0 | 0.0 |
| non_positive_votes | title_ratings | 1730885 | 0 | 0.0 |
| orphan_rating_title | title_ratings | 1730885 | 0 | 0.0 |
| orphan_principal_title | title_principals | 102649609 | 0 | 0.0 |
| episode_parent_not_series | title_episode | 10025088 | 0 | 0.0 |
| duplicate_aka_ordering | title_akas | 60321594 | 0 | 0.0 |
| orphan_episode_title | title_episode | 10025088 | 0 | 0.0 |
| duplicate_tconst | title_basics | 12951113 | 0 | 0.0 |
| end_before_start | title_basics | 12951113 | 0 | 0.0 |
| born_in_future | name_basics | 15802420 | 0 | 0.0 |

Examples of orphan records (first rows printed by the script):

orphan_writer:

| nconst | tconst |
| --- | --- |
| nm17754776 | tt38622790 |
| nm17754865 | tt38623867 |
| nm17756206 | tt38625427 |
| nm17757535 | tt38687716 |
| nm17760655 | tt32627433 |

orphan_episode_parent:

| parentTconst | tconst |
| --- | --- |
| tt28606619 | tt28611150 |
| tt28606619 | tt28638568 |
| tt28606619 | tt29195389 |
| tt28606619 | tt33366211 |
| tt28606619 | tt34954661 |

orphan_aka_title:

| titleId | ordering |
| --- | --- |
| tt50809271 | 1 |
| tt50809271 | 2 |

There is one row per check. `issue_rows` is the number of rows that fail the check and `issue_pct` their share of the table. The 27 checks fall into five families: impossible dates (death before birth, lifespan over 120 years, end year before start year, birth or death in the future), orphan foreign keys found with anti-joins, duplicate keys, out-of-range values (ratings outside 1 to 10, votes of 0 or less, invalid isAdult, runtime of 0 or less or over 1,000 minutes) and inconsistencies between tables (a credit before the person's birth, a release year in the future).

No check goes above 0.05% of its table. The highest share is start_in_future, with 6,017 titles (0.0465% of title_basics). The highest count is orphan_principal_name, with 8,441 credits that point to a person missing from name_basics. Duplicate keys, ratings out of range, non-positive votes and invalid isAdult values never occur. Overall the data is clean and the problems we found are rare.

Most orphans look like a timing issue rather than a real error. The orphan ids in the examples are very recent (nm1775... and nm1776... for the writers, tt50809271 for the akas), so they were probably added to one IMDb file after another file had been exported. The five orphan episodes all point to the same parent, tt28606619, which is not in title_basics.

The check credit_before_birth (3,089 rows) already leaves out archive footage, so the remaining cases are more likely wrong birth years or wrong credits. start_in_future (6,017 rows) is not always an error, because IMDb lists announced movies with their planned year.

### 1.4 Handling of the issues

Treatment applied by `jobs/preparation/clean_imdb.py`:

| problem | rows | treatment |
| --- | --- | --- |
| orphan_principal_name | 8441 | row dropped from title_principals |
| orphan_writer | 1149 | id removed from the writers list |
| orphan_director | 423 | id removed from the directors list |
| orphan_known_for_title | 69 | id removed from knownForTitles |
| orphan_crew_title | 30 | row dropped |
| orphan_episode_parent | 5 | row dropped |
| orphan_aka_title | 2 | row dropped |
| death_before_birth | 28 | birthYear and deathYear set to null + flag |
| implausible_lifespan | 2 | birthYear and deathYear set to null + flag |
| non_positive_runtime | 4 | runtimeMinutes set to null |
| extreme_runtime | 441 | kept, flag is_extreme_runtime |
| start_in_future | 6017 | kept, flag is_future_release |
| credit_before_birth | 3089 | kept, flag is_credit_before_birth |

Rows dropped when building the clean layer:

| table | raw_rows | clean_rows | dropped_rows |
| --- | --- | --- | --- |
| title_basics | 12951113 | 12951113 | 0 |
| name_basics | 15802420 | 15802420 | 0 |
| title_ratings | 1730885 | 1730885 | 0 |
| title_crew | 12949786 | 12949756 | 30 |
| title_episode | 10025088 | 10025083 | 5 |
| title_principals | 102649609 | 102641168 | 8441 |
| title_akas | 60321594 | 60321592 | 2 |

The first table gives the decision for each problem, the second one the number of rows before and after cleaning.

We applied three rules. When a record points to something that does not exist, we drop the row (orphan credits, crew rows, episodes and akas), or, when the id sits inside a list, we only remove that id from the list (writers, directors, knownForTitles). Such a row cannot be used in a join anyway. When a value is impossible, we set it to null, as for the four runtimes of 0 or less. For a death year before the birth year we set both years to null, because we cannot know which one is wrong, and we add a flag. The two implausible lifespans get the same treatment. When a value is unusual but possible, we keep it and add a flag: extreme runtimes, future releases and credits before birth.

The dropped rows match the check counts exactly: 30 rows in title_crew, 5 in title_episode, 8,441 in title_principals and 2 in title_akas, and none in the other tables. The largest loss, in title_principals, is below 0.01% of the table. Besides these treatments, the clean layer replaces `\N` with real nulls, casts numeric columns to numeric types and turns comma-separated lists into arrays.

### 1.5 Methodological explanation

#### Methodology and why it was chosen

<!-- Jade: ingestion part -->

For the missing values, we read the raw layer, where all columns are text and `\N` is kept on purpose at ingestion. This is what lets us separate real nulls, markers and empty strings. All the counts of a table are computed in a single aggregation, so each table is read only once.

We kept the diagnosis (`jobs/preparation/quality_checks.py`) separate from the cleaning (`jobs/preparation/clean_imdb.py`). The first script only counts the problems and prints examples, it changes nothing. We could look at the numbers and the examples before choosing a treatment, and check afterwards that the cleaning removed exactly what the diagnosis had found. Casts use `try_cast` because Spark 4 runs in ANSI mode, where a normal cast on a bad value stops the job instead of returning null.

The treatment follows one idea: remove only what cannot be used and keep what is unusual. Orphans break joins, so they go. Impossible values become null instead of being guessed. Extreme but possible values stay with a flag, because some questions, Q1 in particular, look for exactly these cases.

#### Alternatives considered and trade-offs

<!-- Jade: ingestion part -->

Converting `\N` to null at ingestion would have been simpler, but we could no longer tell where a missing value comes from. Counting only Spark nulls on the raw layer would have given 0% missing everywhere, which is wrong.

For the cleaning, we could have dropped every row with a problem. That loses information for little gain, and it would have removed the very long movies that Q1 studies. Imputing values (a median runtime, an estimated birth year) means inventing data, which is worse than a null for an analysis. Not cleaning at all would leave orphan keys that distort joins and counts.

Some thresholds are our own choice, like 120 years for a lifespan or 1,000 minutes for a runtime. Other values would change the counts a little, but every check concerns less than 0.05% of rows, so the effect on the clean tables stays small. Future releases are also not always errors, which is why we flag them instead of removing them.

## 2. Data exploration

### 2.1 Q1: runtime outliers per genre

Summary of the two outlier rules (461,965 feature-length movies):

| method | k | outlier_movies | pct_of_movies | shorter_rows |
| --- | --- | --- | --- | --- |
| zscore | 3.0 | 4914 | 1.06 | 0 |
| iqr | 1.5 | 24119 | 5.22 | 5432 |

Per genre, z-score rule (k = 3):

| genre | n_movies | mean_runtime | std_runtime | q1 | median_runtime | q3 | lower_bound | upper_bound | n_outliers | n_longer | n_shorter | pct_outliers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Action | 40332 | 101.0 | 29.8 | 85 | 94 | 113 | 11.6 | 190.4 | 344 | 344 | 0 | 0.85 |
| Adult | 6122 | 79.3 | 26.3 | 66 | 77 | 85 | 0.4 | 158.2 | 79 | 79 | 0 | 1.29 |
| Adventure | 24033 | 92.9 | 29.3 | 79 | 90 | 101 | 5.0 | 180.8 | 316 | 316 | 0 | 1.31 |
| Animation | 8310 | 82.6 | 26.8 | 68 | 81 | 93 | 2.2 | 163.0 | 89 | 89 | 0 | 1.07 |
| Biography | 16103 | 89.2 | 29.4 | 69 | 88 | 104 | 1.0 | 177.4 | 165 | 165 | 0 | 1.02 |
| Comedy | 96591 | 93.0 | 21.7 | 82 | 91 | 102 | 27.9 | 158.1 | 985 | 985 | 0 | 1.02 |
| Crime | 34621 | 95.5 | 25.6 | 83 | 92 | 105 | 18.7 | 172.3 | 271 | 271 | 0 | 0.78 |
| Documentary | 116092 | 76.5 | 28.8 | 57 | 74 | 90 | -9.9 | 162.9 | 1210 | 1210 | 0 | 1.04 |
| Drama | 196594 | 95.9 | 26.6 | 81 | 93 | 107 | 16.1 | 175.7 | 1595 | 1595 | 0 | 0.81 |
| Family | 15934 | 92.1 | 25.6 | 77 | 90 | 102 | 15.3 | 168.9 | 127 | 127 | 0 | 0.8 |
| Fantasy | 12706 | 93.7 | 26.2 | 80 | 91 | 104 | 15.1 | 172.3 | 129 | 129 | 0 | 1.02 |
| Film-Noir | 877 | 81.9 | 13.8 | 71 | 82 | 91 | 40.5 | 123.3 | 3 | 3 | 0 | 0.34 |
| Game-Show | 8 | 80.5 | 32.8 | 52 | 67 | 87 | -17.9 | 178.9 | 0 | 0 | 0 | 0.0 |
| History | 13630 | 93.6 | 36.8 | 72 | 90 | 108 | -16.8 | 204.0 | 133 | 133 | 0 | 0.98 |
| Horror | 30705 | 88.5 | 20.9 | 79 | 88 | 96 | 25.8 | 151.2 | 251 | 251 | 0 | 0.82 |
| Music | 13967 | 87.7 | 30.2 | 68 | 87 | 100 | -2.9 | 178.3 | 169 | 169 | 0 | 1.21 |
| Musical | 8502 | 99.9 | 30.0 | 82 | 94 | 111 | 9.9 | 189.9 | 62 | 62 | 0 | 0.73 |
| Mystery | 16462 | 93.9 | 24.8 | 81 | 92 | 105 | 19.5 | 168.3 | 117 | 117 | 0 | 0.71 |
| News | 1408 | 77.6 | 32.2 | 60 | 75 | 88 | -19.0 | 174.2 | 19 | 19 | 0 | 1.35 |
| Reality-TV | 296 | 87.7 | 33.4 | 64 | 90 | 96 | -12.5 | 187.9 | 7 | 7 | 0 | 2.36 |
| Romance | 42432 | 99.1 | 25.1 | 85 | 95 | 110 | 23.8 | 174.4 | 295 | 295 | 0 | 0.7 |
| Sci-Fi | 9799 | 91.0 | 27.2 | 79 | 90 | 100 | 9.4 | 172.6 | 77 | 77 | 0 | 0.79 |
| Sport | 6817 | 88.5 | 29.2 | 71 | 90 | 100 | 0.9 | 176.1 | 71 | 71 | 0 | 1.04 |
| Talk-Show | 104 | 87.6 | 33.6 | 60 | 79 | 112 | -13.2 | 188.4 | 1 | 1 | 0 | 0.96 |
| Thriller | 34054 | 96.1 | 21.8 | 85 | 93 | 105 | 30.7 | 161.5 | 285 | 285 | 0 | 0.84 |
| War | 8046 | 94.8 | 31.3 | 80 | 92 | 105 | 0.9 | 188.7 | 83 | 83 | 0 | 1.03 |
| Western | 6377 | 76.5 | 28.7 | 57 | 70 | 90 | -9.6 | 162.6 | 101 | 101 | 0 | 1.58 |

Per genre, IQR rule (k = 1.5):

| genre | n_movies | mean_runtime | std_runtime | q1 | median_runtime | q3 | lower_bound | upper_bound | n_outliers | n_longer | n_shorter | pct_outliers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Action | 40332 | 101.0 | 29.8 | 85 | 94 | 113 | 43.0 | 155.0 | 1885 | 1878 | 7 | 4.67 |
| Adult | 6122 | 79.3 | 26.3 | 66 | 77 | 85 | 37.5 | 113.5 | 353 | 353 | 0 | 5.77 |
| Adventure | 24033 | 92.9 | 29.3 | 79 | 90 | 101 | 46.0 | 134.0 | 1463 | 1308 | 155 | 6.09 |
| Animation | 8310 | 82.6 | 26.8 | 68 | 81 | 93 | 30.5 | 130.5 | 218 | 218 | 0 | 2.62 |
| Biography | 16103 | 89.2 | 29.4 | 69 | 88 | 104 | 16.5 | 156.5 | 325 | 325 | 0 | 2.02 |
| Comedy | 96591 | 93.0 | 21.7 | 82 | 91 | 102 | 52.0 | 132.0 | 7253 | 4623 | 2630 | 7.51 |
| Crime | 34621 | 95.5 | 25.6 | 83 | 92 | 105 | 50.0 | 138.0 | 2100 | 1762 | 338 | 6.07 |
| Documentary | 116092 | 76.5 | 28.8 | 57 | 74 | 90 | 7.5 | 139.5 | 2073 | 2073 | 0 | 1.79 |
| Drama | 196594 | 95.9 | 26.6 | 81 | 93 | 107 | 42.0 | 146.0 | 7710 | 7682 | 28 | 3.92 |
| Family | 15934 | 92.1 | 25.6 | 77 | 90 | 102 | 39.5 | 139.5 | 884 | 884 | 0 | 5.55 |
| Fantasy | 12706 | 93.7 | 26.2 | 80 | 91 | 104 | 44.0 | 140.0 | 516 | 513 | 3 | 4.06 |
| Film-Noir | 877 | 81.9 | 13.8 | 71 | 82 | 91 | 41.0 | 121.0 | 3 | 3 | 0 | 0.34 |
| Game-Show | 8 | 80.5 | 32.8 | 52 | 67 | 87 | -0.5 | 139.5 | 1 | 1 | 0 | 12.5 |
| History | 13630 | 93.6 | 36.8 | 72 | 90 | 108 | 18.0 | 162.0 | 417 | 417 | 0 | 3.06 |
| Horror | 30705 | 88.5 | 20.9 | 79 | 88 | 96 | 53.5 | 121.5 | 2435 | 1326 | 1109 | 7.93 |
| Music | 13967 | 87.7 | 30.2 | 68 | 87 | 100 | 20.0 | 148.0 | 381 | 381 | 0 | 2.73 |
| Musical | 8502 | 99.9 | 30.0 | 82 | 94 | 111 | 38.5 | 154.5 | 489 | 489 | 0 | 5.75 |
| Mystery | 16462 | 93.9 | 24.8 | 81 | 92 | 105 | 45.0 | 141.0 | 495 | 491 | 4 | 3.01 |
| News | 1408 | 77.6 | 32.2 | 60 | 75 | 88 | 18.0 | 130.0 | 28 | 28 | 0 | 1.99 |
| Reality-TV | 296 | 87.7 | 33.4 | 64 | 90 | 96 | 16.0 | 144.0 | 13 | 13 | 0 | 4.39 |
| Romance | 42432 | 99.1 | 25.1 | 85 | 95 | 110 | 47.5 | 147.5 | 2184 | 1997 | 187 | 5.15 |
| Sci-Fi | 9799 | 91.0 | 27.2 | 79 | 90 | 100 | 47.5 | 131.5 | 590 | 416 | 174 | 6.02 |
| Sport | 6817 | 88.5 | 29.2 | 71 | 90 | 100 | 27.5 | 143.5 | 185 | 185 | 0 | 2.71 |
| Talk-Show | 104 | 87.6 | 33.6 | 60 | 79 | 112 | -18.0 | 190.0 | 0 | 0 | 0 | 0.0 |
| Thriller | 34054 | 96.1 | 21.8 | 85 | 93 | 105 | 55.0 | 135.0 | 2529 | 1733 | 796 | 7.43 |
| War | 8046 | 94.8 | 31.3 | 80 | 92 | 105 | 42.5 | 142.5 | 359 | 358 | 1 | 4.46 |
| Western | 6377 | 76.5 | 28.7 | 57 | 70 | 90 | 7.5 | 139.5 | 133 | 133 | 0 | 2.09 |

Most extreme outliers, movies over 1,000 minutes excluded (same with both rules):

| genre | tconst | primaryTitle | startYear | runtimeMinutes | mean_runtime | z_score | direction |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Comedy | tt19711676 | Kahrlton Heights North | 2022 | 900 | 93.0 | 37.19 | longer |
| Crime | tt9900018 | His Last Stand | NULL | 990 | 95.5 | 34.94 | longer |
| Adult | tt15095204 | 16 Hours of MILFS 3 | 2020 | 960 | 79.3 | 33.49 | longer |
| Documentary | tt39163316 | The Story of Documentary Film | 2026 | 960 | 76.5 | 30.68 | longer |
| Documentary | tt5982192 | From Stage to Screen | 2016 | 900 | 76.5 | 28.59 | longer |
| Horror | tt1433078 | Amy's Night Out | 2007 | 675 | 88.5 | 28.06 | longer |
| Drama | tt2330754 | My Reflected Death | 2012 | 842 | 95.9 | 28.05 | longer |
| Drama | tt1910611 | Saving Julian | 2012 | 840 | 95.9 | 27.97 | longer |
| Documentary | tt0095979 | The Journey | 1987 | 873 | 76.5 | 27.66 | longer |
| Mystery | tt0246135 | Out 1 | 1971 | 776 | 93.9 | 27.5 | longer |
| Animation | tt39246012 | Tom and Jerry: The Golden Era Anthology | 1958 | 817 | 82.6 | 27.4 | longer |
| Fantasy | tt9047474 | La Flor | 2018 | 808 | 93.7 | 27.26 | longer |
| Drama | tt9047474 | La Flor | 2018 | 808 | 95.9 | 26.77 | longer |
| Sport | tt2000421 | The New York Yankees: Fall Classic Collector's Edition 1996-2001 | 2005 | 865 | 88.5 | 26.59 | longer |
| Documentary | tt2330754 | My Reflected Death | 2012 | 842 | 76.5 | 26.58 | longer |
| Documentary | tt1356735 | Crude Oil | 2008 | 840 | 76.5 | 26.51 | longer |
| Documentary | tt30886362 | exergue - on documenta 14 | 2024 | 840 | 76.5 | 26.51 | longer |
| Drama | tt32186666 | Tendrils | 2024 | 799 | 95.9 | 26.43 | longer |
| Comedy | tt33336291 | Phonetag | 2023 | 659 | 93.0 | 26.08 | longer |
| Romance | tt30249764 | The Man of Her Dreams | 2023 | 750 | 99.1 | 25.93 | longer |

There is one table per rule, with one row per genre (mean, standard deviation, quartiles, bounds and number of outliers), then the 20 most extreme cases.

Out of 461,965 feature-length movies, the z-score rule (k = 3) flags 4,914 movies (1.06%) and the IQR rule (k = 1.5) flags 24,119 (5.22%). A movie with several genres is tested in each of them.

With the z-score rule, every outlier is longer than usual for its genre and no movie is flagged as shorter. The lower bound is under 40 minutes in every genre except Film-Noir (40.5), and it is even negative in eight genres, while a feature-length movie lasts at least 40 minutes by definition. The distributions are also skewed to the right: the mean is above the median in 24 of the 27 genres, which points to a long tail of long movies. In genres with more than 1,000 movies, the share of outliers stays between 0.70% and 1.58%.

The IQR rule does find short movies: 5,432 genre rows are flagged as shorter, mostly in Comedy (2,630), Horror (1,109) and Thriller (796). These genres have a narrow spread. Horror, for example, goes from 79 to 96 minutes between q1 and q3, so its lower bound is 53.5 minutes and a 50 minute horror movie is already an outlier.

The most extreme cases last between 659 and 990 minutes. Some look like compilations rather than films, judging by their titles (16 Hours of MILFS 3, Tom and Jerry: The Golden Era Anthology, The New York Yankees: Fall Classic Collector's Edition 1996-2001). Others are real long films, such as Out 1 (1971, 776 minutes) and La Flor (2018, 808 minutes). This confirms the choice made at cleaning time to flag extreme runtimes instead of removing them.

### 2.2 Q2 and Q3: top three genres per year and comparison with the rest

Q2: top three genres per year, popularity = total numVotes:

| year | genre_1 | genre_2 | genre_3 | votes_1 | votes_2 | votes_3 |
| --- | --- | --- | --- | --- | --- | --- |
| 1980 | Drama | Adventure | Horror | 3164819 | 2482632 | 2074123 |
| 1981 | Adventure | Drama | Horror | 2251402 | 1711745 | 1276820 |
| 1982 | Drama | Sci-Fi | Action | 3074765 | 2458969 | 1873945 |
| 1983 | Drama | Action | Adventure | 2922076 | 2025147 | 1985018 |
| 1984 | Action | Drama | Adventure | 3669079 | 3024936 | 2932761 |
| 1985 | Comedy | Adventure | Drama | 4566038 | 3412174 | 3078672 |
| 1986 | Drama | Adventure | Comedy | 4000288 | 2966714 | 2787177 |
| 1987 | Drama | Comedy | Action | 4137788 | 3688550 | 2361692 |
| 1988 | Drama | Comedy | Action | 4466311 | 4432598 | 2715298 |
| 1989 | Comedy | Drama | Adventure | 5014420 | 3963006 | 2944536 |
| 1990 | Drama | Comedy | Crime | 6324682 | 4017794 | 3590073 |
| 1991 | Drama | Crime | Comedy | 5588018 | 3675900 | 3195891 |
| 1992 | Drama | Comedy | Crime | 6099279 | 4075428 | 3611172 |
| 1993 | Drama | Comedy | Crime | 8999217 | 4501354 | 3047242 |
| 1994 | Drama | Crime | Comedy | 15747059 | 6507403 | 5679622 |
| 1995 | Drama | Crime | Comedy | 12528617 | 7768146 | 6083744 |
| 1996 | Drama | Comedy | Crime | 7863994 | 4791130 | 4446357 |
| 1997 | Drama | Action | Comedy | 12634373 | 6770157 | 5607096 |
| 1998 | Drama | Comedy | Crime | 11786967 | 9866252 | 6220552 |
| 1999 | Drama | Comedy | Crime | 15528313 | 8225246 | 7184803 |
| 2000 | Drama | Comedy | Adventure | 13027658 | 8337823 | 6993744 |
| 2001 | Drama | Comedy | Adventure | 14742137 | 9516341 | 9256622 |
| 2002 | Drama | Adventure | Action | 17066773 | 8695817 | 8397456 |
| 2003 | Drama | Action | Comedy | 13259253 | 11713343 | 10762498 |
| 2004 | Drama | Action | Comedy | 16479020 | 11523490 | 10951297 |
| 2005 | Drama | Comedy | Action | 13694127 | 10195984 | 9062571 |
| 2006 | Drama | Comedy | Thriller | 20891703 | 11778993 | 8667776 |
| 2007 | Drama | Comedy | Action | 18540785 | 11719971 | 8996584 |
| 2008 | Drama | Action | Comedy | 17579894 | 13412064 | 11330987 |
| 2009 | Drama | Comedy | Action | 17596531 | 13466462 | 12119997 |
| 2010 | Drama | Action | Adventure | 17559263 | 13337999 | 13183274 |
| 2011 | Drama | Action | Comedy | 20751128 | 17150981 | 13984334 |
| 2012 | Drama | Action | Comedy | 20121163 | 14898126 | 12512415 |
| 2013 | Drama | Action | Comedy | 21989798 | 17190251 | 14959670 |
| 2014 | Drama | Action | Adventure | 24522622 | 20941532 | 17744552 |
| 2015 | Drama | Action | Adventure | 20495612 | 15564331 | 14166566 |
| 2016 | Drama | Action | Adventure | 20892137 | 17211205 | 12366736 |
| 2017 | Drama | Action | Comedy | 20809907 | 16713853 | 11307025 |
| 2018 | Drama | Action | Adventure | 17414719 | 15082375 | 13350891 |
| 2019 | Drama | Action | Comedy | 22027617 | 12361151 | 12268703 |
| 2020 | Drama | Comedy | Action | 10479712 | 6516147 | 6130494 |
| 2021 | Drama | Action | Adventure | 14400589 | 12875939 | 10191771 |
| 2022 | Drama | Action | Comedy | 15972881 | 13356033 | 10206198 |
| 2023 | Drama | Action | Comedy | 14767394 | 10584001 | 9500983 |
| 2024 | Drama | Action | Comedy | 11673990 | 8653432 | 7096136 |
| 2025 | Drama | Action | Comedy | 8641549 | 7695383 | 5909126 |

Q3: top three genres compared with the other genres, per year:

| year | n_other_genres | top_vote_share_pct | top_movie_share_pct | top_votes_per_movie | rest_votes_per_movie | top_avg_rating | rest_avg_rating |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1980 | 21 | 44.3 | 37.0 | 5699.0 | 4207.0 | 7.73 | 7.5 |
| 1981 | 20 | 46.5 | 35.7 | 3884.0 | 2484.0 | 7.24 | 6.85 |
| 1982 | 19 | 43.7 | 38.6 | 5032.0 | 4072.0 | 7.56 | 7.15 |
| 1983 | 19 | 45.9 | 41.0 | 4327.0 | 3558.0 | 7.43 | 7.11 |
| 1984 | 19 | 44.1 | 39.4 | 6199.0 | 5094.0 | 7.35 | 7.19 |
| 1985 | 19 | 51.5 | 46.8 | 5973.0 | 4943.0 | 7.39 | 7.02 |
| 1986 | 19 | 46.2 | 47.3 | 5018.0 | 5233.0 | 7.2 | 7.05 |
| 1987 | 19 | 42.6 | 52.9 | 4437.0 | 6701.0 | 7.09 | 7.06 |
| 1988 | 19 | 46.8 | 51.4 | 5180.0 | 6225.0 | 7.17 | 7.07 |
| 1989 | 19 | 49.5 | 45.7 | 5821.0 | 4998.0 | 7.22 | 6.78 |
| 1990 | 19 | 45.6 | 50.3 | 6441.0 | 7771.0 | 7.3 | 7.07 |
| 1991 | 19 | 44.2 | 50.3 | 5973.0 | 7638.0 | 7.36 | 7.49 |
| 1992 | 19 | 46.3 | 50.5 | 6921.0 | 8183.0 | 7.23 | 7.0 |
| 1993 | 19 | 44.7 | 49.8 | 8499.0 | 10398.0 | 7.31 | 7.39 |
| 1994 | 19 | 58.3 | 50.9 | 13573.0 | 10079.0 | 7.87 | 7.32 |
| 1995 | 19 | 49.0 | 48.4 | 12983.0 | 12699.0 | 7.59 | 7.28 |
| 1996 | 20 | 44.9 | 49.3 | 8278.0 | 9876.0 | 6.98 | 6.78 |
| 1997 | 19 | 44.1 | 52.1 | 10776.0 | 14862.0 | 7.09 | 7.13 |
| 1998 | 19 | 51.5 | 50.9 | 11932.0 | 11646.0 | 7.4 | 6.8 |
| 1999 | 19 | 43.6 | 51.2 | 12267.0 | 16609.0 | 7.53 | 7.32 |
| 2000 | 19 | 45.0 | 46.0 | 11646.0 | 12142.0 | 7.21 | 6.98 |
| 2001 | 19 | 42.4 | 46.7 | 12492.0 | 14886.0 | 7.36 | 7.16 |
| 2002 | 22 | 42.4 | 37.9 | 15332.0 | 12689.0 | 7.22 | 7.04 |
| 2003 | 21 | 45.5 | 50.6 | 11483.0 | 14096.0 | 7.11 | 7.22 |
| 2004 | 21 | 41.3 | 48.8 | 11909.0 | 16112.0 | 7.02 | 7.11 |
| 2005 | 21 | 40.4 | 48.0 | 9317.0 | 12706.0 | 6.79 | 6.87 |
| 2006 | 20 | 42.7 | 47.1 | 10559.0 | 12640.0 | 7.12 | 6.95 |
| 2007 | 21 | 38.3 | 45.0 | 9939.0 | 13103.0 | 6.96 | 6.91 |
| 2008 | 20 | 40.4 | 45.3 | 9483.0 | 11596.0 | 6.75 | 7.1 |
| 2009 | 20 | 41.3 | 44.8 | 8565.0 | 9866.0 | 6.98 | 6.88 |
| 2010 | 21 | 40.0 | 33.0 | 10751.0 | 7946.0 | 6.97 | 7.02 |
| 2011 | 21 | 46.1 | 42.7 | 9050.0 | 7897.0 | 6.84 | 6.75 |
| 2012 | 21 | 42.7 | 41.8 | 7958.0 | 7651.0 | 6.9 | 6.96 |
| 2013 | 21 | 41.5 | 41.1 | 8319.0 | 8168.0 | 6.85 | 6.95 |
| 2014 | 23 | 47.0 | 30.8 | 12005.0 | 6017.0 | 7.09 | 6.96 |
| 2015 | 21 | 46.6 | 31.8 | 9267.0 | 4944.0 | 6.98 | 6.77 |
| 2016 | 21 | 44.1 | 32.1 | 9155.0 | 5483.0 | 6.89 | 6.9 |
| 2017 | 22 | 45.0 | 41.7 | 6693.0 | 5851.0 | 6.93 | 6.8 |
| 2018 | 22 | 44.3 | 32.5 | 8045.0 | 4869.0 | 6.94 | 6.74 |
| 2019 | 22 | 43.6 | 41.9 | 6274.0 | 5840.0 | 7.0 | 6.85 |
| 2020 | 22 | 44.7 | 38.9 | 4246.0 | 3351.0 | 6.42 | 6.41 |
| 2021 | 22 | 46.4 | 32.3 | 7572.0 | 4160.0 | 6.78 | 6.54 |
| 2022 | 22 | 46.4 | 41.7 | 5353.0 | 4418.0 | 6.75 | 6.56 |
| 2023 | 22 | 44.8 | 42.9 | 4497.0 | 4172.0 | 6.67 | 6.66 |
| 2024 | 23 | 42.9 | 41.3 | 3776.0 | 3540.0 | 6.57 | 6.52 |
| 2025 | 22 | 39.3 | 41.4 | 3406.0 | 3713.0 | 6.51 | 6.46 |

Q3: number of years each genre is in the top three:

| genre | years_in_top | years_first | first_year | last_year |
| --- | --- | --- | --- | --- |
| Drama | 46 | 42 | 1980 | 2025 |
| Comedy | 34 | 2 | 1985 | 2025 |
| Action | 29 | 1 | 1982 | 2025 |
| Adventure | 16 | 1 | 1980 | 2021 |
| Crime | 9 | 0 | 1990 | 1999 |
| Horror | 2 | 0 | 1980 | 1981 |
| Thriller | 1 | 0 | 2006 | 2006 |
| Sci-Fi | 1 | 0 | 1982 | 1982 |

Q3: averages by decade:

| decade | avg_top_vote_share_pct | avg_top_movie_share_pct | avg_top_rating | avg_rest_rating |
| --- | --- | --- | --- | --- |
| 1980 | 46.1 | 43.6 | 7.34 | 7.08 |
| 1990 | 47.2 | 50.4 | 7.37 | 7.16 |
| 2000 | 42.0 | 46.0 | 7.05 | 7.02 |
| 2010 | 44.1 | 36.9 | 6.94 | 6.87 |
| 2020 | 44.1 | 39.7 | 6.62 | 6.52 |

Alternative ranking by number of movies (`--metric movies`), top three per year:

| year | genre_1 | genre_2 | genre_3 | votes_1 | votes_2 | votes_3 |
| --- | --- | --- | --- | --- | --- | --- |
| 1980 | Drama | Comedy | Action | 3164819 | 1545562 | 508762 |
| 1981 | Drama | Comedy | Action | 1711745 | 1028176 | 932126 |
| 1982 | Drama | Comedy | Action | 3074765 | 1358514 | 1873945 |
| 1983 | Drama | Comedy | Action | 2922076 | 1617155 | 2025147 |
| 1984 | Drama | Comedy | Action | 3024936 | 2762987 | 3669079 |
| 1985 | Drama | Comedy | Action | 3078672 | 4566038 | 1677472 |
| 1986 | Drama | Comedy | Action | 4000288 | 2787177 | 2635517 |
| 1987 | Drama | Comedy | Action | 4137788 | 3688550 | 2361692 |
| 1988 | Drama | Comedy | Action | 4466311 | 4432598 | 2715298 |
| 1989 | Drama | Comedy | Action | 3963006 | 5014420 | 1815211 |
| 1990 | Drama | Comedy | Action | 6324682 | 4017794 | 2874383 |
| 1991 | Drama | Comedy | Action | 5588018 | 3195891 | 3080249 |
| 1992 | Drama | Comedy | Action | 6099279 | 4075428 | 2262006 |
| 1993 | Drama | Comedy | Action | 8999217 | 4501354 | 2209649 |
| 1994 | Drama | Comedy | Action | 15747059 | 5679622 | 4023306 |
| 1995 | Drama | Comedy | Action | 12528617 | 6083744 | 4869691 |
| 1996 | Drama | Comedy | Action | 7863994 | 4791130 | 4026927 |
| 1997 | Drama | Comedy | Romance | 12634373 | 5607096 | 5068048 |
| 1998 | Drama | Comedy | Romance | 11786967 | 9866252 | 3013274 |
| 1999 | Drama | Comedy | Romance | 15528313 | 8225246 | 3674379 |
| 2000 | Drama | Comedy | Romance | 13027658 | 8337823 | 3484962 |
| 2001 | Drama | Comedy | Romance | 14742137 | 9516341 | 5015533 |
| 2002 | Drama | Comedy | Documentary | 17066773 | 7294482 | 422477 |
| 2003 | Drama | Comedy | Documentary | 13259253 | 10762498 | 306968 |
| 2004 | Drama | Comedy | Documentary | 16479020 | 10951297 | 466954 |
| 2005 | Drama | Comedy | Documentary | 13694127 | 10195984 | 419907 |
| 2006 | Drama | Comedy | Documentary | 20891703 | 11778993 | 503298 |
| 2007 | Drama | Comedy | Documentary | 18540785 | 11719971 | 430592 |
| 2008 | Drama | Comedy | Documentary | 17579894 | 11330987 | 644325 |
| 2009 | Drama | Documentary | Comedy | 17596531 | 473677 | 13466462 |
| 2010 | Drama | Documentary | Comedy | 17559263 | 777875 | 12861265 |
| 2011 | Drama | Documentary | Comedy | 20751128 | 696417 | 13984334 |
| 2012 | Drama | Documentary | Comedy | 20121163 | 745707 | 12512415 |
| 2013 | Drama | Documentary | Comedy | 21989798 | 688269 | 14959670 |
| 2014 | Drama | Documentary | Comedy | 24522622 | 815428 | 14427801 |
| 2015 | Drama | Documentary | Comedy | 20495612 | 891814 | 11179200 |
| 2016 | Drama | Documentary | Comedy | 20892137 | 814479 | 11620243 |
| 2017 | Drama | Documentary | Comedy | 20809907 | 809689 | 11307025 |
| 2018 | Drama | Documentary | Comedy | 17414719 | 854605 | 10405368 |
| 2019 | Drama | Documentary | Comedy | 22027617 | 822417 | 12268703 |
| 2020 | Drama | Documentary | Comedy | 10479712 | 899118 | 6516147 |
| 2021 | Drama | Documentary | Comedy | 14400589 | 713728 | 7677555 |
| 2022 | Drama | Documentary | Comedy | 15972881 | 902077 | 10206198 |
| 2023 | Drama | Documentary | Comedy | 14767394 | 644171 | 9500983 |
| 2024 | Drama | Documentary | Comedy | 11673990 | 701221 | 7096136 |
| 2025 | Drama | Documentary | Comedy | 8641549 | 532765 | 5909126 |

Alternative ranking, number of years in the top three:

| genre | years_in_top | years_first | first_year | last_year |
| --- | --- | --- | --- | --- |
| Drama | 46 | 46 | 1980 | 2025 |
| Comedy | 46 | 0 | 1980 | 2025 |
| Documentary | 24 | 0 | 2002 | 2025 |
| Action | 17 | 0 | 1980 | 1996 |
| Romance | 5 | 0 | 1997 | 2001 |

Alternative ranking, averages by decade:

| decade | avg_top_vote_share_pct | avg_top_movie_share_pct | avg_top_rating | avg_rest_rating |
| --- | --- | --- | --- | --- |
| 1980 | 40.9 | 51.9 | 7.18 | 7.2 |
| 1990 | 43.5 | 52.7 | 7.27 | 7.26 |
| 2000 | 32.0 | 54.3 | 7.04 | 7.03 |
| 2010 | 29.9 | 51.3 | 6.93 | 6.89 |
| 2020 | 30.6 | 50.2 | 6.67 | 6.53 |

The Q2 table gives the top three genres of each year with their votes. The Q3 tables compare the top three with the other genres (share of votes, share of movies, votes per movie, ratings), then count the years each genre is in the top three and give averages by decade. The last three tables repeat this with the ranking by number of movies.

Drama is the most voted genre in 42 of the 46 years and is in the top three every year. The four exceptions are 1981 (Adventure), 1984 (Action), 1985 and 1989 (Comedy). Comedy is in the top three in 34 years and Action in 29. Crime only appears between 1990 and 1999 (9 years, all but 1997). Since 2010 Action is second in 15 of the 16 years, the exception being 2020, when Comedy passes it. 1994 stands out: Drama gets 15.7 million votes against 9.0 million in 1993, and the top three takes 58.3% of the votes, the highest share of the period. In absolute terms Drama peaks in 2014, with 24.5 million votes. One nuance applies to all these results: Drama is a very broad label (196,594 of the 461,965 movies in the Q1 base), and a movie with three genres counts in all three.

For Q3, the top three is three genres out of 22 to 26, but it takes between 38% and 52% of the votes each year (58.3% in 1994). Comparing votes and number of movies shows two periods. From 1986 to 2009, the top three has fewer votes per movie than the other genres in 19 of the 24 years, probably because Drama and Comedy, which lead in those years, include many movies with few votes. From 2010 the order reverses and the top three has more votes per movie in 15 of the 16 years. The gap is largest in the years where Adventure is in the top three instead of Comedy (2010, 2014, 2015, 2016, 2018, 2021). The top three then makes only 30.8% to 33.0% of the movies for 40.0% to 47.0% of the votes, with 1.35 to 2 times more votes per movie than the rest. Votes concentrate on a smaller group of large action and adventure productions.

The ratings of the top three are only a little higher than the rest, by 0.03 to 0.26 points depending on the decade, and not every year (6.75 against 7.10 in 2008). Ratings go down over time, from 7.34 in the 1980s to 6.62 in the 2020s for the top three. A likely reason is survival bias: the old movies still rated on IMDb today are mostly the ones people remember.

Ranking by number of movies measures production instead of audience. Drama is first every year and Comedy is in the top three every year. The third place goes to Action until 1996, to Romance from 1997 to 2001 and to Documentary from 2002, which then becomes second from 2009. Documentary is made in large numbers but gets few votes, between 0.3 and 0.9 million a year against 8.6 to 24.5 million for Drama over the same years. After 2000, the top three by production makes about half of the movies (50.2% to 54.3% by decade) but only about 30% of the votes (29.9% to 32.0%). The two rankings answer different questions. We keep the votes ranking as our answer to "popularity".

### 2.3 Q4: directors with exactly one movie rated 8.0 or more

Number of matching directors by vote threshold:

| min_votes | directors_with_at_least_one_movie | matching_Q4 |
| --- | --- | --- |
| 1000 | 23220 | 48 |
| 100 | 66172 | 168 |

Main result (at least 1,000 votes), 30 most voted hits out of 48:

| nconst | primaryName | hit_title | hit_year | hit_rating | hit_votes | n_others | others_avg | others_min | others_max | gap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0449984 | Irvin Kershner | Star Wars: Episode V - The Empire Strikes Back | 1980 | 8.7 | 1532729 | 9 | 5.94 | 4.5 | 6.8 | 2.76 |
| nm0574625 | James McTeigue | V for Vendetta | 2005 | 8.1 | 1261462 | 4 | 5.98 | 5.5 | 6.4 | 2.12 |
| nm0796117 | M. Night Shyamalan | The Sixth Sense | 1999 | 8.2 | 1176914 | 15 | 5.87 | 3.9 | 7.3 | 2.33 |
| nm0446819 | Richard Kelly | Donnie Darko | 2001 | 8.0 | 925161 | 2 | 5.45 | 5.3 | 5.6 | 2.55 |
| nm0001675 | Robert Rodriguez | Sin City | 2005 | 8.0 | 825646 | 22 | 5.94 | 3.6 | 7.5 | 2.06 |
| nm0588340 | Frank Miller | Sin City | 2005 | 8.0 | 825646 | 2 | 5.65 | 4.8 | 6.5 | 2.35 |
| nm0132709 | Martin Campbell | Casino Royale | 2006 | 8.0 | 758545 | 16 | 5.92 | 4.2 | 7.2 | 2.08 |
| nm0853238 | Tate Taylor | The Help | 2011 | 8.1 | 534971 | 6 | 5.88 | 5.0 | 6.9 | 2.22 |
| nm0204628 | Garth Davis | Lion | 2016 | 8.0 | 270263 | 2 | 5.65 | 5.4 | 5.9 | 2.35 |
| nm0490487 | Wai Keung Lau | Infernal Affairs | 2002 | 8.0 | 139886 | 21 | 6.0 | 2.7 | 7.4 | 2.0 |
| nm1749112 | Vincent Paronnaud | Persepolis | 2007 | 8.0 | 107476 | 3 | 5.7 | 4.8 | 7.0 | 2.3 |
| nm0000671 | Billy Bob Thornton | Sling Blade | 1996 | 8.0 | 105204 | 3 | 5.93 | 5.7 | 6.3 | 2.07 |
| nm0031342 | Predrag Antonijevic | Dara of Jasenovac | 2020 | 8.0 | 82505 | 5 | 5.7 | 4.3 | 7.4 | 2.3 |
| nm1084488 | Umesh Shukla | OMG: Oh My God! | 2012 | 8.1 | 72448 | 6 | 5.58 | 3.6 | 7.4 | 2.52 |
| nm2134474 | Vikas Bahl | Queen | 2013 | 8.1 | 72014 | 6 | 5.83 | 2.6 | 7.9 | 2.27 |
| nm1326535 | Sundar C. | Anbe Sivam | 2003 | 8.6 | 28824 | 10 | 5.52 | 4.1 | 7.5 | 3.08 |
| nm1729063 | Mert Baykal | Pardon | 2005 | 8.1 | 24933 | 3 | 5.9 | 4.5 | 7.4 | 2.2 |
| nm0846092 | Kamal Tabrizi | The Lizard | 2004 | 8.4 | 24812 | 4 | 5.53 | 4.1 | 7.5 | 2.87 |
| nm5120912 | Ajay Bahl | Section 375 | 2019 | 8.0 | 21936 | 3 | 4.63 | 1.8 | 6.3 | 3.37 |
| nm0244892 | J.P. Dutta | Border | 1997 | 8.0 | 20775 | 4 | 5.33 | 5.2 | 5.5 | 2.67 |
| nm1359360 | Dharani | Ghilli | 2004 | 8.2 | 19431 | 5 | 5.44 | 3.9 | 7.2 | 2.76 |
| nm8678191 | Karthick Naren | Dhuruvangal Pathinaaru | 2016 | 8.1 | 19410 | 2 | 5.15 | 4.4 | 5.9 | 2.95 |
| nm0700872 | Puri Jagannadh | Pokiri | 2006 | 8.0 | 17304 | 18 | 5.8 | 2.6 | 7.4 | 2.2 |
| nm5650589 | Nalan Kumarasamy | Soodhu Kavvum | 2013 | 8.2 | 13769 | 2 | 5.95 | 4.4 | 7.5 | 2.25 |
| nm0149134 | Sinan Çetin | Abbas in Flower | 1982 | 8.0 | 12165 | 7 | 4.84 | 1.8 | 7.1 | 3.16 |
| nm0348015 | Gunasekhar | Okkadu | 2003 | 8.0 | 11852 | 6 | 5.38 | 3.6 | 7.2 | 2.62 |
| nm1703622 | R. Madhesh | Mudhalvan | 1999 | 8.4 | 9591 | 3 | 5.8 | 5.1 | 6.5 | 2.6 |
| nm1318819 | Ezhil | Thulladha Manamum Thullum | 1999 | 8.3 | 7361 | 2 | 4.55 | 2.8 | 6.3 | 3.75 |
| nm6044360 | Ravikanth Perepu | Kshanam | 2016 | 8.1 | 6708 | 2 | 5.95 | 5.9 | 6.0 | 2.15 |
| nm0803350 | Sangeeth Sivan | Yoddha | 1992 | 8.4 | 5496 | 4 | 5.25 | 3.5 | 6.1 | 3.15 |

Largest gaps between the hit and the other movies (1,000 votes):

| nconst | primaryName | hit_title | hit_year | hit_rating | hit_votes | n_others | others_avg | others_min | others_max | gap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm6252943 | Harish Vyas | Om Ka Hari | 2026 | 9.8 | 5025 | 2 | 5.75 | 4.8 | 6.7 | 4.05 |
| nm1318819 | Ezhil | Thulladha Manamum Thullum | 1999 | 8.3 | 7361 | 2 | 4.55 | 2.8 | 6.3 | 3.75 |
| nm2735803 | Saurabh Varma | The Great Punjab Robbery | 2026 | 9.3 | 1758 | 2 | 5.9 | 5.7 | 6.1 | 3.4 |
| nm0389514 | David Hogan | Sign 'o' the Times | 1987 | 8.0 | 3054 | 2 | 4.6 | 3.6 | 5.6 | 3.4 |
| nm5120912 | Ajay Bahl | Section 375 | 2019 | 8.0 | 21936 | 3 | 4.63 | 1.8 | 6.3 | 3.37 |
| nm0062614 | Harry Baweja | Chaar Sahibzaade 2: Rise of Banda Singh Bahadur | 2016 | 8.6 | 1051 | 6 | 5.3 | 2.7 | 7.8 | 3.3 |
| nm0149134 | Sinan Çetin | Abbas in Flower | 1982 | 8.0 | 12165 | 7 | 4.84 | 1.8 | 7.1 | 3.16 |
| nm0803350 | Sangeeth Sivan | Yoddha | 1992 | 8.4 | 5496 | 4 | 5.25 | 3.5 | 6.1 | 3.15 |
| nm0002239 | Prince | Sign 'o' the Times | 1987 | 8.0 | 3054 | 2 | 4.9 | 4.7 | 5.1 | 3.1 |
| nm1326535 | Sundar C. | Anbe Sivam | 2003 | 8.6 | 28824 | 10 | 5.52 | 4.1 | 7.5 | 3.08 |

Sensitivity check (at least 100 votes), 30 most voted hits out of 168:

| nconst | primaryName | hit_title | hit_year | hit_rating | hit_votes | n_others | others_avg | others_min | others_max | gap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0574625 | James McTeigue | V for Vendetta | 2005 | 8.1 | 1261462 | 4 | 5.98 | 5.5 | 6.4 | 2.12 |
| nm0549658 | Richard Marquand | Star Wars: Episode VI - Return of the Jedi | 1983 | 8.3 | 1229284 | 6 | 5.98 | 4.4 | 7.1 | 2.32 |
| nm0796117 | M. Night Shyamalan | The Sixth Sense | 1999 | 8.2 | 1176914 | 15 | 5.87 | 3.9 | 7.3 | 2.33 |
| nm0446819 | Richard Kelly | Donnie Darko | 2001 | 8.0 | 925161 | 3 | 5.43 | 5.3 | 5.6 | 2.57 |
| nm0001675 | Robert Rodriguez | Sin City | 2005 | 8.0 | 825646 | 23 | 5.9 | 3.6 | 7.5 | 2.1 |
| nm0588340 | Frank Miller | Sin City | 2005 | 8.0 | 825646 | 2 | 5.65 | 4.8 | 6.5 | 2.35 |
| nm0132709 | Martin Campbell | Casino Royale | 2006 | 8.0 | 758545 | 20 | 5.63 | 4.2 | 7.2 | 2.37 |
| nm0000814 | John G. Avildsen | Rocky | 1976 | 8.1 | 707111 | 21 | 5.78 | 3.9 | 7.4 | 2.32 |
| nm0853238 | Tate Taylor | The Help | 2011 | 8.1 | 534971 | 6 | 5.88 | 5.0 | 6.9 | 2.22 |
| nm0936374 | Kirk Wise | Beauty and the Beast | 1991 | 8.0 | 511389 | 3 | 5.43 | 2.4 | 7.0 | 2.57 |
| nm0204628 | Garth Davis | Lion | 2016 | 8.0 | 270263 | 2 | 5.65 | 5.4 | 5.9 | 2.35 |
| nm0490487 | Wai Keung Lau | Infernal Affairs | 2002 | 8.0 | 139886 | 43 | 5.72 | 2.7 | 7.4 | 2.28 |
| nm1749112 | Vincent Paronnaud | Persepolis | 2007 | 8.0 | 107476 | 4 | 5.83 | 4.8 | 7.0 | 2.17 |
| nm0000671 | Billy Bob Thornton | Sling Blade | 1996 | 8.0 | 105204 | 3 | 5.93 | 5.7 | 6.3 | 2.07 |
| nm1084488 | Umesh Shukla | OMG: Oh My God! | 2012 | 8.1 | 72448 | 6 | 5.58 | 3.6 | 7.4 | 2.52 |
| nm2134474 | Vikas Bahl | Queen | 2013 | 8.1 | 72014 | 6 | 5.83 | 2.6 | 7.9 | 2.27 |
| nm1025280 | Anubhav Sinha | Article 15 | 2019 | 8.1 | 42155 | 12 | 5.78 | 3.5 | 7.6 | 2.32 |
| nm1164861 | Seth Gordon | The King of Kong: A Fistful of Quarters | 2007 | 8.0 | 38795 | 7 | 5.7 | 3.8 | 6.9 | 2.3 |
| nm6134590 | Apoorv Singh Karki | Sirf Ek Bandaa Kaafi Hai | 2023 | 8.0 | 31517 | 2 | 4.2 | 3.7 | 4.7 | 3.8 |
| nm0559877 | John Mathew Matthan | Sarfarosh | 1999 | 8.1 | 29138 | 2 | 5.75 | 5.1 | 6.4 | 2.35 |
| nm1326535 | Sundar C. | Anbe Sivam | 2003 | 8.6 | 28824 | 26 | 5.6 | 3.5 | 7.7 | 3.0 |
| nm0178880 | Eleanor Coppola | Hearts of Darkness: A Filmmaker's Apocalypse | 1991 | 8.1 | 26269 | 3 | 5.83 | 4.6 | 7.1 | 2.27 |
| nm1729063 | Mert Baykal | Pardon | 2005 | 8.1 | 24933 | 3 | 5.9 | 4.5 | 7.4 | 2.2 |
| nm0846092 | Kamal Tabrizi | The Lizard | 2004 | 8.4 | 24812 | 14 | 5.76 | 4.1 | 7.5 | 2.64 |
| nm5120912 | Ajay Bahl | Section 375 | 2019 | 8.0 | 21936 | 3 | 4.63 | 1.8 | 6.3 | 3.37 |
| nm0576554 | Hansal Mehta | Shahid | 2012 | 8.2 | 21890 | 11 | 5.75 | 3.5 | 7.7 | 2.45 |
| nm0244892 | J.P. Dutta | Border | 1997 | 8.0 | 20775 | 9 | 5.94 | 5.2 | 7.3 | 2.06 |
| nm6293294 | Rojin Thomas | #Home | 2021 | 8.7 | 20362 | 2 | 5.95 | 4.5 | 7.4 | 2.75 |
| nm1359360 | Dharani | Ghilli | 2004 | 8.2 | 19431 | 5 | 5.44 | 3.9 | 7.2 | 2.76 |
| nm8678191 | Karthick Naren | Dhuruvangal Pathinaaru | 2016 | 8.1 | 19410 | 3 | 5.43 | 4.4 | 6.0 | 2.67 |

Largest gaps (100 votes):

| nconst | primaryName | hit_title | hit_year | hit_rating | hit_votes | n_others | others_avg | others_min | others_max | gap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm2840629 | Steve Rahaman | To Be King | 2011 | 8.6 | 232 | 2 | 3.0 | 2.6 | 3.4 | 5.6 |
| nm7263882 | Swaylee Loughnane | Mother's Child | 2020 | 8.2 | 6734 | 2 | 2.65 | 2.3 | 3.0 | 5.55 |
| nm10898010 | Robbie Lopez | River Beauty | 2023 | 8.3 | 1217 | 2 | 2.9 | 1.9 | 3.9 | 5.4 |
| nm0464424 | Gábor Koltay | István, a király | 1984 | 8.3 | 1250 | 2 | 3.0 | 2.6 | 3.4 | 5.3 |
| nm1069500 | Sam Logan Khaleghi | Love Is a Thieves' Game | 2011 | 8.6 | 110 | 2 | 3.4 | 3.1 | 3.7 | 5.2 |
| nm1142903 | Vito Trabucco | Henchman: The Al Leong Story | 2018 | 8.3 | 106 | 4 | 3.6 | 2.5 | 4.7 | 4.7 |
| nm3427311 | Joseph Henson | Court of Appeals | 2018 | 8.4 | 114 | 2 | 3.7 | 3.5 | 3.9 | 4.7 |
| nm2551464 | Nayato Fio Nuala | Heart 2 Heart | 2010 | 8.4 | 107 | 3 | 3.87 | 3.0 | 5.1 | 4.53 |
| nm1235947 | Jayprakkash Shaw | Love karu Yaa Shaadi | 2025 | 8.2 | 165 | 4 | 4.0 | 3.2 | 4.8 | 4.2 |
| nm10584889 | Shhivaji Doltade | Raja Rani | 2024 | 9.0 | 106 | 2 | 4.8 | 2.3 | 7.3 | 4.2 |

The `hit_*` columns describe the movie rated 8.0 or more. `n_others` is the number of other movies of the director, `others_avg`, `others_min` and `others_max` their ratings, and `gap` the rating of the hit minus the average of the others.

With at least 1,000 votes per movie, 48 of the 23,220 directors match the rule. With 100 votes, there are 168 out of 66,172.

The 1,000 votes list contains well-known cases. Irvin Kershner directed The Empire Strikes Back (8.7, 1.5 million votes), and his nine other movies average 5.94. James McTeigue has V for Vendetta (8.1) and four other movies at 5.98. M. Night Shyamalan has The Sixth Sense (8.2) and fifteen other movies at 5.87, between 3.9 and 7.3. Richard Kelly has Donnie Darko (8.0) and two other movies rated 5.3 and 5.6. The gap between the hit and the rest goes from 2.0 to 4.05 points. The largest one is for Om Ka Hari (2026, rated 9.8), a very recent movie.

The vote threshold changes the result a lot. At 100 votes, seven of the ten largest gaps come from hits rated by only 106 to 232 people, such as To Be King (8.6 with 232 votes) next to two movies rated 2.6 and 3.4. These ratings are too fragile, which is why the 1,000 votes list is our main result. The 1,000 votes list is also not included in the 100 votes list. Kershner is in the first one but not in the second, and Richard Marquand (Return of the Jedi) is only in the second. A lower threshold adds movies to each director, so a director can gain a second hit or see the average of the other movies go above 6.0. This explains why Kershner drops out at 100 votes. The opposite also happens: a higher threshold removes the movies with fewer than 1,000 votes, so a director can be left with fewer than two other movies, or the average of the remaining ones can change. This is how Marquand can match at 100 votes and not at 1,000. For Wai Keung Lau, for example, the number of other movies goes from 21 to 43.

### 2.4 Q5: series with the worst episode in the last season

<!-- Melchior -->

### 2.5 Q6: actors and actresses successful in several languages

<!-- Melchior -->

### 2.6 Q7: careers that peaked early

<!-- Melchior -->

### 2.7 Q8: multi-talented professionals

Multi-talented people by career combination of roles (11,028 in total):

| career_combination | count |
| --- | --- |
| director+writer | 7910 |
| actor+director+writer | 1466 |
| actor+writer | 1210 |
| actor+director | 442 |

Paired comparison, movies with several roles minus movies with one role (each person compared with themselves):

| n_people | mean_diff | median_diff | pct_better_when_multi |
| --- | --- | --- | --- |
| 8398 | 0.047 | 0.054 | 53.2 |

Pooled view by combination of roles held on the same movie:

| movie_combination | n_person_movies | avg_rating | median_votes |
| --- | --- | --- | --- |
| director+writer | 46048 | 6.01 | 647 |
| actor | 38878 | 5.89 | 910 |
| director | 20824 | 5.96 | 643 |
| writer | 18973 | 5.94 | 862 |
| actor+director+writer | 4926 | 5.66 | 546 |
| actor+writer | 3827 | 5.69 | 856 |
| actor+director | 1176 | 5.76 | 520 |

People with the most multi-role movies:

| primaryName | career_combination | n_multi | n_single | multi_avg | single_avg | diff |
| --- | --- | --- | --- | --- | --- | --- |
| Jesús Franco | actor+director+writer | 127 | 24 | 4.65 | 4.45 | 0.21 |
| Jing Wong | actor+director+writer | 91 | 71 | 5.65 | 5.72 | -0.07 |
| Yōji Yamada | director+writer | 63 | 7 | 6.89 | 6.87 | 0.02 |
| Giannis Dalianidis | director+writer | 61 | 12 | 5.98 | 5.94 | 0.04 |
| Jean-Luc Godard | actor+director+writer | 60 | 12 | 6.43 | 6.14 | 0.29 |
| Carlo Vanzina | director+writer | 55 | 13 | 4.85 | 4.83 | 0.02 |
| Woody Allen | actor+director+writer | 54 | 8 | 6.89 | 5.69 | 1.2 |
| Cheh Chang | director+writer | 53 | 22 | 6.47 | 6.33 | 0.14 |
| Claude Chabrol | actor+director+writer | 47 | 24 | 6.54 | 6.08 | 0.46 |
| Godfrey Ho | actor+director+writer | 45 | 23 | 4.12 | 4.51 | -0.39 |
| Kader Khan | actor+writer | 45 | 126 | 5.73 | 5.35 | 0.38 |
| Claude Lelouch | director+writer | 44 | 7 | 6.3 | 6.56 | -0.26 |
| Neri Parenti | director+writer | 44 | 3 | 4.87 | 5.1 | -0.23 |
| Werner Herzog | actor+director+writer | 44 | 10 | 7.09 | 6.48 | 0.61 |
| Mario Monicelli | director+writer | 44 | 34 | 6.67 | 6.61 | 0.06 |
| Dino Risi | director+writer | 42 | 15 | 6.35 | 6.57 | -0.21 |
| Julien Duvivier | director+writer | 42 | 7 | 6.84 | 6.77 | 0.06 |
| Steno | director+writer | 41 | 37 | 6.19 | 6.02 | 0.17 |
| Alberto Sordi | actor+director+writer | 41 | 63 | 6.14 | 6.65 | -0.5 |
| Alekos Sakellarios | director+writer | 41 | 16 | 6.96 | 5.93 | 1.03 |

The first table counts multi-talented people by the combination of roles over their career. The second one summarises the paired comparison. The third gives ratings by the combination of roles held on the same movie, and the last one lists the people with the most multi-role movies.

We find 11,028 multi-talented people. Most of them combine directing and writing (7,910). 1,466 hold the three roles, 1,210 are actor and writer, and 442 are actor and director.

To see whether people do better when they hold several roles on the same movie, we compare each person with themselves: the average rating of their multi-role movies minus the average of their single-role movies. 8,398 people have both kinds of movies. The mean difference is +0.047 and the median +0.054, and 53.2% of them do better when they combine roles. The effect is close to zero.

The pooled view, which puts all the person-movie pairs together, gives a different picture. Movies where the person acts and also directs or writes are rated 5.66 to 5.76, below single-role movies (5.89 to 5.96), while director+writer movies are the best rated group (6.01). Our hypothesis, which we did not measure directly, is that the lower ratings of the acting combinations come in part from a few very prolific people with low ratings: Jesús Franco alone has 127 multi-role movies at 4.65, and Godfrey Ho 45 at 4.12. The paired comparison removes this effect, since each person is only compared with themselves. Movies where the person both acts and directs also get fewer votes (median 520 and 546, against 643 to 910 for the other groups), which suggests more personal, smaller projects.

Individual profiles vary a lot. Woody Allen gains 1.2 points when he combines roles, Alekos Sakellarios 1.03, Werner Herzog 0.61 and Claude Chabrol 0.46. Alberto Sordi loses 0.5 and Godfrey Ho 0.39.

### 2.8 Methodological explanation

#### Methodology and why it was chosen

For Q1, we keep titles of type movie lasting 40 minutes or more, the threshold used for a feature film by the Academy of Motion Picture Arts and Sciences, the AFI and the BFI, and we leave out movies with a future release year. A movie is tested in each of its genres. We compute the mean and standard deviation of each genre and flag a movie when its runtime is more than three standard deviations from the mean. With k = 3 about 1% of movies are flagged, which fits the idea of a runtime "notably different" from the genre. Movies over 1,000 minutes are left out when we compute the statistics, because a few of them would inflate the standard deviation, but they are still tested.

For Q2, popularity is the total numVotes of the movies of a genre in a given year. We rank genres within each year and never build a ranking that mixes several years, because votes add up over time and older movies have had longer to collect them. Within one year, all movies have had about the same time. Comparing totals between years, as we do for 1993 and 1994 or 2019 and 2020, is still possible, but it has to be read with care for the same reason. For Q3, we compare the top three with the other genres on the share of votes, the share of movies, the votes per movie and the average rating, then average these figures by decade to make the trend easier to read.

For Q4, we use feature-length movies with at least 1,000 votes, so that each rating rests on enough voters. A director matches when exactly one movie is rated 8.0 or more and the simple average of the other movies is 6.0 or less. We require at least two other movies, otherwise the "average" would be a single rating. We run the same query at 100 votes as a sensitivity check.

For Q8, the director and writer roles come from title_crew, which is complete, and the actor role from title_principals. A person is multi-talented when they hold at least two roles, each on at least three movies, which removes one-off cameos. We only keep movies with at least 100 votes. The main measure is the paired comparison, because comparing each person with themselves removes the differences of talent and style between people.

<!-- Melchior: Q5, Q6, Q7 -->

#### Alternatives considered and trade-offs

For Q1, the main alternative is the IQR rule. It is based on quartiles, so extreme values do not pull the bounds, and it finds short outliers that the z-score misses. But it flags 5.22% of movies, which is a lot for a "notable" difference. A third option is a z-score on the logarithm of the runtime, which would reduce the skew of the distribution. With both rules, small genres such as Game-Show (8 movies) and Talk-Show (104) give unstable bounds.

For Q2 and Q3, counting movies instead of votes measures production rather than audience, and gives a different top three, with Documentary from 2002. The median of votes per movie would be another option, less sensitive to a few very popular movies. Our votes ranking has its own limits. A movie with several genres counts in each of them, the last years (2024 and 2025) are still collecting votes, and 2020 shows a clear dip (10.5 million votes for Drama, against 22.0 million in 2019), probably linked to Covid.

For Q4, an average weighted by votes would give more weight to the popular movies of each director. We kept the simple average because the question is about the average rating of the movies. Co-directors are counted separately, so Sin City appears for both Robert Rodriguez and Frank Miller, and Sign 'o' the Times for David Hogan and Prince. Very recent movies can have high ratings at the start of their life (Om Ka Hari, 2026, 9.8). The 8.0 cut is also sharp: Vikas Bahl matches with Queen (8.1), but one of his other movies is rated 7.9.

For Q8, the pooled view is simpler, but as the results show it is biased by a few very prolific people, so we only use it for comparison. To select multi-talented people, we could have used primaryProfession from name_basics, but this field is declarative and does not say what the person really did on each movie, so we preferred the actual credits. title_principals only lists the main cast, so smaller acting roles are missing. The multi-role group also mixes director+writer with the combinations that include acting, and the threshold of three movies per role is arbitrary.

<!-- Melchior: Q5, Q6, Q7 -->

## 3. Data enrichment

### 3.1 Scope and results

<!-- Melchior -->

### 3.2 Sources, provenance and rate limits

<!-- Melchior -->

### 3.3 Methodological explanation

#### Methodology and why it was chosen

<!-- Melchior -->

#### Alternatives considered and trade-offs

<!-- Melchior -->

## 4. Streaming data processing

### 4.1 Pipeline

<!-- Jade -->

### 4.2 Results and streaming metrics

<!-- Jade -->

### 4.3 Late arriving data

<!-- Jade -->

### 4.4 Methodological explanation

#### Methodology and why it was chosen

<!-- Jade -->

#### Alternatives considered and trade-offs

<!-- Jade -->

## 5. Open question

### 5.1 Question, interest and data

<!-- To decide -->

### 5.2 Results and key takeaways

<!-- To decide -->

### 5.3 Methodological explanation

#### Methodology and why it was chosen

<!-- Jade -->

#### Alternatives considered and trade-offs

<!-- Jade -->
