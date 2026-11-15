# ECE Big Data Processing: IMDb project report

Group gr-01. Setup and run instructions are in [PROJECT.md](PROJECT.md).

## 1. Data preparation

### 1.1 Ingestion

<!-- Jade -->

### 1.2 Null, missing and empty values

<!-- Adam -->

### 1.3 Other data quality issues

<!-- Adam -->

### 1.4 Handling of the issues

<!-- Adam -->

### 1.5 Methodological explanation

#### Methodology and why it was chosen

<!-- Adam (Jade for the ingestion part) -->

#### Alternatives considered and trade-offs

<!-- Adam (Jade for the ingestion part) -->

## 2. Data exploration

### 2.1 Q1: runtime outliers per genre

<!-- Adam -->

### 2.2 Q2 and Q3: top three genres per year and comparison with the rest

<!-- Adam -->

### 2.3 Q4: directors with exactly one movie rated 8.0 or more

<!-- Adam -->

### 2.4 Q5: series with the worst episode in the last season

We are looking for series of the tvSeries or tvMiniSeries type. A series is eligible if it has at least 3 seasons, each with at least 10 rated episodes.
An episode is counted if it has at least 100 votes. 
We then compare all the numbered seasons of the series; the worst episode must be in the last season, and the best episode in season 1 or 2.
Season 0, which consists of special episodes, is ignored.

 Version table :

| Version | Candidates | Matching | Share |
|---|---:|---:|---:|
| All series (final) | 998 | 155 | 15.5% |
| Ended series only | 863 | 145 | 16.8% |
| Running series (difference) | 135 | 10 | 7.4% |

In the end, there are 155 out of 998 eligible series (15.5%), and if we look only at the completed series, there are 145 out of 863.

 Table of the Top 10 series that correspond

| Series | Start | End | Last season | Best season | Best rating | Worst season | Worst rating |
|---|---:|---:|---:|---:|---:|---:|---:|
| House of Cards | 2013 | 2018 | 6 | 2 | 9.5 | 6 | 2.5 |
| Narcos | 2015 | 2017 | 3 | 2 | 9.4 | 3 | 7.8 |
| The Flash | 2014 | 2023 | 9 | 1 | 9.6 | 9 | 3.8 |
| You | 2018 | 2025 | 5 | 2 | 9.1 | 5 | 5.3 |
| 13 Reasons Why | 2017 | 2020 | 4 | 1 | 9.1 | 4 | 5.1 |
| The Umbrella Academy | 2019 | 2024 | 4 | 2 | 8.9 | 4 | 4.8 |
| Two and a Half Men | 2003 | 2015 | 12 | 2 | 8.7 | 12 | 3.6 |
| True Blood | 2008 | 2014 | 7 | 2 | 8.8 | 7 | 5.4 |
| Heroes | 2006 | 2010 | 4 | 1 | 8.9 | 4 | 6.8 |
| Jessica Jones | 2015 | 2019 | 3 | 1 | 9.0 | 3 | 7.1 |

We can see that there are some series known for having declined in quality. *House of Cards*, *The Umbrella Academy*, and *Silicon Valley* all had short final seasons, during which their worst episodes aired.

Some series are still running, meaning their latest known season is not necessarily their final season. These ongoing series meet the criterion much less frequently than completed series (7.4% versus 16.8%). *The Rookie*, *Shrinking*, and *One Punch Man* are excluded from the list when we consider only completed series.

*Game of Thrones* has its worst episode in Season 8 (4.0), but its best in Season 6 (9.9). It meets only one criterion, so it does not appear on the list.

### 2.5 Q6: actors and actresses successful in several languages

A movie is considered a hit if it has a rating of at least 7 and at least 10,000 votes, for a total of 4,969 movies. Its original language is taken from Wikidata (property P364). Only the `actor` and `actress` categories are counted. Movies with multiple original languages are not included.

617 actors have achieved success in at least 2 languages, and 62 in at least 3

| Votes threshold | Films used | English share | ≥ 2 languages | ≥ 3 languages |
|---:|---:|---:|---:|---:|
| 10,000 | 4,292 | 65.1% | 617 | 62 |
| 1,000 | 11,552 | 44.5% | 2,061 | 310 |

The most common language combinations are English/French (81 actors), Hindi/Tamil (49), English/Italian (49), English/German (39), and Malayalam/Tamil (35).
Two distinct patterns emerge: international careers in Europe, and Indian actors who perform in several of the country’s languages.

Here are some successful actors who work in four languages: 
Prakash Raj (Hindi, Kannada, Tamil, Telugu), Ashish Vidyarthi, Sobhita Dhulipala, Bruno Ganz (English, German, Greek, Italian), Franco Nero (English, German, Italian, Spanish).

At 10,000 votes, 65% of the films included are in English. At 1,000 votes, that figure drops to 45%. Non-English-language films receive fewer votes and are less likely to reach the threshold. At 1,000 votes, multilingual Indian cinema dominates (Atul Kulkarni, 6 languages), as do certain European actors (Bruno Ganz, 6; Max von Sydow, 5).

Of the 4,969 films, 84 have no usable language, or about 1.7%

### 2.6 Q7: careers that peaked early

We chose from a `movie` type film with at least 1,000 votes and an `actor` and `actress` category.
For a career to be eligible, there must be at least 20 years between the first and last film counted, and at least 15 films.
A career peaks early if three conditions are met: the best film is in the first third of the career, the average rating of the early films exceeds that of the later ones by at least 0.5 points, and there are at least 3 films at the beginning and at least 3 afterward.
The beginning of the career is the first movie with at least 1000 votes. 

Of the 3,529 careers, 921 reach their peak early.

| Measure | Raw rating | Rating minus yearly mean |
|---|---:|---:|
| Eligible careers | 3,529 | 3,529 |
| Peaked early | 921 (26.1%) | 613 (17.4%) |
| Peaked late (control) | 47 (1.3%) | 100 (2.8%) |
| Negative rating trend | 2,841 (80.5%) | 2,425 (68.7%) |

We applied the same definition in reverse (best film in the last third of the career, higher average rating toward the end). Only 47 careers (1.3%) peaked late, compared with 921 that peaked early—about 20 times fewer. And 80.5% of careers saw their ratings decline over the years.

On IMDb, older movies receive higher ratings probably because only those that have stood the test of time still receive votes. So we compared each rating to the average for films released in the same year. The result: 613 careers peaked early and 100 peaked late. The gap narrows (about 6 to 1 instead of 20 to 1), but it remains significant. Part of the effect, then, comes from the era—but not all of it.


The 10 careers with the greatest difference between starting and subsequent scores, in raw scores.

| Name | Career | Movies | Early avg | Later avg | Drop | Best movie |
|---|---|---:|---:|---:|---:|---|
| Geetha | 1983-2025 | 17 | 8.52 | 5.91 | 2.62 | Sagara Sangamam (1983) |
| Halit Akçatepe | 1972-2005 | 24 | 7.82 | 5.26 | 2.56 | The Chaos Class (1975) |
| James Duval | 1993-2024 | 22 | 6.51 | 4.06 | 2.46 | Donnie Darko (2001) |
| Steve Brodie | 1947-1988 | 20 | 6.83 | 4.38 | 2.45 | Out of the Past (1947) |
| Jason London | 1991-2024 | 19 | 6.40 | 4.16 | 2.24 | Dazed and Confused (1993) |
| Ranjeet Bedi | 1979-2017 | 17 | 6.92 | 4.69 | 2.23 | Satte Pe Satta (1982) |
| Corey Feldman | 1984-2021 | 15 | 6.26 | 4.05 | 2.21 | Stand by Me (1986) |
| Richard Tyson | 1987-2025 | 19 | 6.26 | 4.05 | 2.21 | Three O'Clock High (1987) |
| John Phillip Law | 1966-2002 | 15 | 6.22 | 4.03 | 2.19 | The Russians Are Coming the Russians Are Coming (1966) |
| Madhan Bob | 1992-2021 | 16 | 7.80 | 5.61 | 2.19 | Thevar Magan (1992) |

There are actors who rose to fame at a young age thanks to a cult classic, but who went on to star only in lower-rated films: Corey Feldman (*Stand by Me*), Edward Furlong (*Terminator 2*), Jason London (*Dazed and Confused*), and James Duval (*Donnie Darko*). There are also many Turkish and Indian actors whose classics from the ’70s and ’80s are highly rated, probably by a nostalgic domestic audience.

### 2.7 Q8: multi-talented professionals

<!-- Adam -->

### 2.8 Methodological explanation

#### Methodology and why it was chosen

For Q5, Q6 and Q7, the raw data layer is used directly. Each query handles \N values and missing ratings independently, using `try_cast`. The difference compared with the clean data layer affects less than 0.01% of rows.


For Q5, an episode is only counted if it has at least 100 votes. This prevents an episode rated by only a handful of viewers from becoming the highest- or lowest-rated episode.
A series is eligible if it has at least three seasons, each containing at least 10 rated episodes.
All numbered seasons are then compared, including shorter seasons, since final seasons are often shorter and are frequently disappointing (for example House of Cards, Season 6).
In case of a tie, the episode with the highest number of votes is selected. If a tie remains, the episode ID is used as a final tiebreaker to ensure consistent results across runs.


For Q6, a movie is considered successful if it has a rating of at least 7 and at least 10,000 votes, ensuring that its rating is reliable and that the movie has reached a sufficiently large audience.
The original language is retrieved from Wikidata (P364), because the `language` column in `title.akas` refers to the language of each translated title, not the movie's original language.
Movies with multiple original languages are excluded; otherwise, a single movie could contribute several languages to the same actor's language profile.
This ensures that each language attributed to an actor comes from a different movie.
Language names are standardized before counting, since Wikidata sometimes uses different labels for the same language. Without this standardization, 40 actors were incorrectly counted as bilingual because of separate entries for "American English" and "English".


For Q7, we count movies of type `movie` with at least 1,000 votes for the `actor` and `actress` categories. A career is eligible if it spans at least 20 years and includes at least 15 movies.
We combine three conditions (the highest-rated movie must fall within the first third of the career, the difference in average ratings must be at least 0.5, and there must be at least three movies both before and after that point) to avoid relying on a single movie.
The minimum number of movies after the turning point was added because one actress had only two movies afterwards, making her average rating unreliable.
We also apply the same definition in reverse (late-career peak) to provide a point of comparison. Without this benchmark, it is difficult to determine whether 26.1% is a high or low proportion.
Finally, we compare each movie's rating with the average rating for its release year, since older movies are probably rated more highly on IMDb, and the beginning of a career is necessarily earlier than its later stages.

#### Alternatives considered and trade-offs

For Q5, the first version compared only seasons with at least 10 episodes. This excluded shorter final seasons and produced a result of 18.0% instead of 15.5%.
Using a threshold of 10 votes per episode retained more series (2,727 instead of 998), but also allowed extreme ratings based on very few votes to influence the results.
Ongoing series can distort the results because their latest known season is not necessarily their actual final season. They meet the criterion much less frequently than completed series (7.4% versus 16.8%).
Therefore, both versions are presented: one including all series and the other including only completed series.


For Q6, using `title.akas` would have avoided relying on an external source, but it does not provide the original language. One drawback of using Wikidata is that 84 out of 4,969 movies have no usable language information.
Including multilingual movies would have retained 598 additional movies, but it would no longer be possible to determine which language came from which movie.
With a threshold of 1,000 votes, the proportion of English-language movies drops from 65% to 45%, showing that the 10,000-vote threshold favours English-language cinema.
Both thresholds are therefore presented (617 and 2,061 actors with at least two languages, respectively).
Grouping Serbian, Croatian and Bosnian under Serbo-Croatian is debatable, since they are officially recognized as separate languages. However, without this grouping, one actress was counted as having successful movies in four languages, based on four different Wikidata labels that do not represent four distinct languages.


For Q7, using raw ratings, approximately 20 times more careers peak early than late. When ratings are adjusted for release year, the ratio drops to approximately 6 to 1. The effect is therefore reduced but remains substantial.
The adjustment has one limitation: 17 years have fewer than 30 movies available to calculate their average rating, although these years account for only 223 movies.
Using a single criterion (the highest-rated movie alone or the rating trend alone) would have been simpler, but a single exceptional movie could have been enough to determine the result. Combining three criteria makes the selection more robust, although fewer careers qualify.
Success is measured using ratings rather than vote counts, following the example provided in the assignment instructions.
One limitation is that the beginning of a career is defined as the first movie with at least 1,000 votes, rather than the actor's actual first movie. As a result, careers that started with lesser-known movies begin later in our analysis.

## 3. Data enrichment

### 3.1 Scope and results

We have enriched feature-length films that are 40 minutes or longer. The films were released between 1980 and 2025. We have the top 10 by genre and by year, ranked by number of votes. We also list the top 3 particpants(principals) for each film according to `ordering`, along with a synopsis for each film and a biography for each person.

| Step | Count |
|---|---:|
| (genre, year, film) rows selected | 9,832 |
| Distinct films | 6,822 |
| Film-principal pairs | 20,449 |
| Distinct people | 10,153 |
| IMDb IDs mapped through Wikidata | 16,975 |

A movie can rank among the top in several genres, and some genres don't have 10 movies a year.

| Films (per genre, year, film row) | Rows | Share |
|---|---:|---:|
| Synopsis from the Plot section | 8,920 | 90.7% |
| Lead section used as synopsis | 409 | 4.2% |
| No Wikidata item | 254 | 2.6% |
| No English Wikipedia article | 248 | 2.5% |
| Article without usable text | 1 | 0.0% |

| Principals (per film-principal pair) | Rows | Share |
|---|---:|---:|
| Biography retrieved | 17,808 | 87.1% |
| No Wikidata item | 1,761 | 8.6% |
| No English Wikipedia article | 879 | 4.3% |
| Empty article | 1 | 0.0% |

We can see from these tables that 95% of the rows have a synopsis, 90.7% of which are taken from an actual “Plot” section. 
The most common reason for a missing biography is the absence of a Wikidata entry (8.6%).
The second most common reason is the absence of an English-language article (4.3%), likely because supporting actors or crew members do not have English-language pages. 

### 3.2 Sources, provenance and rate limits

Wikidata links the IMDb ID to the Wikipedia article. The English Wikipedia is accessed via the MediaWiki API, which provides the text. All of this text is written by humans.

Each row in the dataset has a `source_url`, which is the article's URL or the Wikidata URI if there is no article (`wikidata_uri`). We also take the `retrieved_at` data for tracability. 
When information is missing, the `note` column explains why.

We followed the API rules by using a User-Agent that included my email address, sending one request at a time, pausing for 1 second between requests, and setting the `maxlag` parameter to 5. We waited longer in the event of a 429 error and the 5xx error.
We also processed in batches of 200 IDs on Wikidata and 20 for biographies.

If the script stops, each result is immediately written to a file, and restarting the script picks up where it left off.
This has already happened to us—at about 4,350 out of 6,822 movies. We were able to resume without any data loss.

### 3.3 Methodological explanation

#### Methodology and why it was chosen

- Spark Selction : Selection of films and individuals. All thresholds are parameters, so the same code can be applied to the entire dataset

- Collect with python : API calls—which is the simplest approach for slow calls, one at a time. Also, the instruction allows for collection in Python

- Wikidata First : We use the IMDb ID instead of searching by title; that way, there's no confusion between two movies with the same name.

- Cleaning Up in Spark: We define the schema, remove duplicates, extract the synopsis from the “Plot” section, and use the automatic notes.We were required to use Spark for data cleaning, and the notes highlight every shortcoming. If there is not "Plot" section, we take the Introduction.

- Storage: Text is stored only once per movie and per person; for example, a movie listed in three genres does not have its synopsis duplicated three times.


#### Alternatives considered and trade-offs

- Top 10 by rating instead of by votes: The problem with this was that highly rated but little-watched movies often don't have Wikipedia pages. But as a result, we're ranking them by popularity rather than by quality.

- Search by title instead of Wikidata: We ruled out searching by title on Wikipedia because two movies can have the same name. But as a result of our decision, if a movie doesn't have an entry in Wikidata, we can't find it at all. This is the case for 254 entries.

- Use Introduction instead of the “Plot” section: The issue we've taken into account here is that an introduction summarizes the movie; it doesn't retell it. But as a result, we have one movie at a time instead of 20 at once, which is slower.

- Extracting the synopsis in Python: We ruled out this option because the instructions specified that we use Spark for data cleaning. So we store the full text, which results in larger files.

We have 409 synopses taken from the article's introduction; these are summaries, not actual synopses.
We used only one source (English Wikipedia), without looking elsewhere when a movie was missing.
Everything depends on Wikidata; if a movie or person isn't listed there, or doesn't have an IMDb ID, it isn't enriched.

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
