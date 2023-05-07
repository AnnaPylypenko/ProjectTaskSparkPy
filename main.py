import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import count, col, desc, sum, floor, row_number, avg, round, dense_rank
from os import path

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Paths to imdb data
name_bas_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/name.basics.tsv.gz")
tit_akas_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.akas.tsv.gz")
tit_bas_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.basics.tsv.gz")
tit_crew_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.crew.tsv.gz")
tit_epis_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.episode.tsv.gz")
tit_prin_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.principals.tsv.gz")
tit_rat_PATH = path.join("imdb_data", "/content/drive/MyDrive/Colab Notebooks/imdb_data/title.ratings.tsv.gz")

name_bas_SCHEMA = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", StringType(), True),
    StructField("deathYear", StringType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])

tit_akas_SCHEMA = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", IntegerType(), True)
])

tit_bas_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", IntegerType(), True),
    StructField("startYear", StringType(), True),
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", StringType(), True),
    StructField("genres", StringType(), True)
])

tit_crew_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])

tit_epis_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", StringType(), True),
    StructField("episodeNumber", StringType(), True)
])

tit_prin_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
])

tit_rat_SCHEMA = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", StringType(), True),
    StructField("numVotes", IntegerType(), True)
])

# Read csv
def d_read(session, file_path, schema, sep="\t", header=True):
    df = session.read.csv(file_path, sep=sep, header=header, schema=schema)
    return df

# Write csv
def d_write(df, directory_to_write):
    df.coalesce(1).write.mode("overwrite").csv(directory_to_write, header=True)

session = SparkSession\
        .builder\
        .appName("IMDB")\
        .getOrCreate()

# Task 1
title_akas_df = d_read(session, tit_akas_PATH, tit_akas_SCHEMA)
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)

ukr_df = title_akas_df.filter(title_akas_df.region == "UA")
df = ukr_df.join(title_basics_df, ukr_df.titleId == title_basics_df.tconst)
df_task_1 = df.select("primaryTitle")

d_write(df_task_1, path.join("results", "task_1"))
print(f"Task 1: Get all titles of series/movies etc. that are available in Ukrainian")
df_task_1.show(truncate=False)

# Task 2
name_basics_df = d_read(session, name_bas_PATH, name_bas_SCHEMA)
born_19th_df = name_basics_df.filter((col("birthYear") >= 1800) & (col("birthYear") < 1900))
names_19th_df_task_2 = born_19th_df.select("primaryName")

d_write(names_19th_df_task_2, path.join("results", "task_2"))
print(f"Task 2: Get the list of people’s names, who were born in the 19th century.")
names_19th_df_task_2.show(truncate=False)

# Task 3
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)
long_movies_df = title_basics_df.filter(col("titleType") == "movie").filter(col("runtimeMinutes") > 120)
res_df_task_3 = long_movies_df.select("primaryTitle")

d_write(res_df_task_3, path.join("results", "task_3"))
print(f"Task 3: Get titles of all movies that last more than 2 hours")
res_df_task_3.show(truncate=False)

# Task 4
name_basics_df = d_read(session, name_bas_PATH, name_bas_SCHEMA)
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)
title_principals_df = d_read(session, tit_prin_PATH, tit_prin_SCHEMA)

joined_df = name_basics_df\
    .join(title_principals_df, name_basics_df["nconst"] == title_principals_df["nconst"])\
    .join(title_basics_df, title_basics_df["tconst"] == title_principals_df["tconst"])

result_df = joined_df.select("primaryName", "primaryTitle", "category", "characters")
result_df_task_4 = result_df.filter(col("category").isin(["actor", "actress"]))

d_write(result_df_task_4, path.join("results", "task_4"))
print(f"Task 4: Get names of people, corresponding movies/series and characters they played in those films")
result_df_task_4.show(truncate=False)

# Task 5
title_akas_df = d_read(session, tit_akas_PATH, tit_akas_SCHEMA)
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)

adult_df = title_basics_df.filter(col("isAdult") == True)
joined_df = title_akas_df.join(adult_df, title_akas_df["titleId"] == adult_df["tconst"])
res_df_task_5 = joined_df\
    .groupBy("region")\
    .agg(count("*").alias("count"))\
    .orderBy(desc("count"))

d_write(res_df_task_5, path.join("results", "task_5"))
print(f"Task 5: Get information about how many adult movies/series etc. there are per region. Get the top 100 of them from the region with the biggest count to the region with the smallest one.")
res_df_task_5.show(truncate=False)

# Task 6
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)
title_episode_df = d_read(session, tit_epis_PATH, tit_epis_SCHEMA)

episode_df = title_episode_df.select("parentTconst", "episodeNumber") \
    .withColumn("episodeNumber", col("episodeNumber").cast(IntegerType()))
num_episodes_df = episode_df.groupBy("parentTconst") \
    .agg(sum("episodeNumber").alias("num_episodes")) \
    .orderBy(col("num_episodes").desc())
tv_series_df = title_basics_df.filter(col("titleType") == "tvSeries") \
    .select("tconst", "primaryTitle")
result_df = tv_series_df.join(num_episodes_df, tv_series_df["tconst"] == num_episodes_df["parentTconst"]) \
    .select("primaryTitle", "num_episodes") \
    .orderBy(col("num_episodes").desc())
top_50_tv_task_6 = result_df.limit(50)

d_write(top_50_tv_task_6, path.join("results", "task_6"))
print(f"Task 6: Get information about how many episodes in each TV Series. Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.")
top_50_tv_task_6.show(truncate=False)

# Task 7
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)
title_ratings_df = d_read(session, tit_rat_PATH, tit_rat_SCHEMA)

title_basics_df = title_basics_df.filter(col("titleType").isin(["movie", "tvSeries"]))
title_basics_df = title_basics_df.withColumn("startYear", title_basics_df["startYear"].cast("int"))
title_basics_df = title_basics_df.withColumn("decade", round((col("startYear") / 10).cast("double")) * 10)
title_data = title_basics_df.join(title_ratings_df, "tconst")
grouped_data = title_data.groupBy("decade", "primaryTitle").agg(avg("averageRating").alias("averageRating"))
window_spec = Window.partitionBy("decade").orderBy(col("averageRating").desc())
ranked_data = grouped_data.withColumn("rank", dense_rank().over(window_spec))
row_number_spec = Window.partitionBy("decade").orderBy(col("averageRating").desc(), col("primaryTitle"))
ranked_data = ranked_data.withColumn("row_number", row_number().over(row_number_spec))
top_titles_task_7 = ranked_data.filter(col("row_number") <= 10).orderBy("decade", "rank")

d_write(top_titles_task_7, path.join("results", "task_7"))
print(f"Task 7: Get 10 titles of the most popular movies/series etc. by each decade")
top_titles_task_7.show(truncate=False)

# Task 8
title_basics_df = d_read(session, tit_bas_PATH, tit_bas_SCHEMA)
title_ratings_df = d_read(session, tit_rat_PATH, tit_rat_SCHEMA)

joined_df = title_basics_df.join(title_ratings_df, on="tconst")
filtered_df = joined_df.filter(joined_df.genres.isNotNull())
grouped_df = filtered_df.groupBy("genres", "primaryTitle").agg(avg("averageRating").alias("averageRating"))
window_spec = Window.partitionBy("genres").orderBy(desc("averageRating"))
ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))
top_10_df = ranked_df.filter(col("rank") <= 10)
top_10_df_task_8 = top_10_df.orderBy("genres", "rank")

d_write(top_10_df_task_8, path.join("results", "task_8"))
print(f"Task 8: Get 10 titles of the most popular movies/series etc. by each genre")
top_10_df_task_8.show(truncate=False)

session.stop()

