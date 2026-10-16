# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f48b752-8972-44bf-8312-1fa1d43a6b76",
# META       "default_lakehouse_name": "bronze_layer",
# META       "default_lakehouse_workspace_id": "cba38f54-3fe7-486e-a341-b3fbdb58bdc2",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f48b752-8972-44bf-8312-1fa1d43a6b76"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

 from pyspark.sql import functions as F
 from pyspark.sql.window import Window
 df = spark.table("bronze_kaggle_full_data_silver_plus")
 df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q1

# CELL ********************

from pyspark.sql import functions as F

#use F. here because its a column expression
df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")
df1.count()
df1.select(F.col("fight_url")).distinct().count()

#They dont output everything, so we have to produce new columns for it to display to see both results
print("Total rows:", df1.count())
print("Unique fight URLs:", df1.select("fight_url").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q2

# CELL ********************

from pyspark.sql import functions as F

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

display(
    df1.agg(
        F.min(F.col("fight_url")),
        F.max(F.col("fight_url")
        ))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q3

# CELL ********************

#`.groupBy().count().filter(...)` (HAVING is just a filter after groupBy)

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

display(
    df1.groupby(("fight_url"))
    .count()
    .filter("count > 1")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q4

# CELL ********************

#| Fights per year                                     | `F.year(...)`, `.groupBy`, `.orderBy`                                                                                       

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

display(
    df1.withColumn("year", F.year("event_date"))
       .groupBy("year")
       .count()
       .orderBy(F.desc("year"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q5

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Q5  | Null % for the same 5 columns                       | `F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))`, then a loop over column names (plain Python building Spark expressions) |
#event_name,referee,winner,num_rounds,fight_url
from pyspark.sql import functions as F

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

#We start out defining a list, of the different columns we want to search
cols = [
    "event_name",
    "referee",
    "winner",
    "num_rounds",
    "fight_url"
]

#We do the null calculation here
#This gives the calculations of the null count for each column in cols.

null_percentages = [
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.count("*") * 100
    ).alias(c)

#Here, we ask it to iterate through and do that for each column
    for c in cols
]

display(
    df1.agg(*null_percentages)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q6

# CELL ********************

# Q6  | DWCS events                                         | `.filter(F.col(...).contains(...) \| ...)` (note: `\|` and `&`, not `or`/`and`, with brackets around each condition)        |
from pyspark.sql import functions as F

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

display(
    df1.filter(
        (F.col("event_name").contains("DWCS")) |
        (F.col("event_name").contains("Contender Series"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q7

# CELL ********************

# Q7  | Fight number per fighter                            | `Window.partitionBy(...).orderBy(...)` + `F.row_number().over(w)`                                                           |
#window = W.partitionBy("first_name", "last_name").orderBy(F.desc("date"))
#df = df.withColumn("row_number", F.row_number().over(window))
#df = df.filter(F.col("row_number") == 1)
#df = df.drop("row_number")
from pyspark.sql.window import Window
from pyspark.sql import functions as F

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")


display(
#We tell the columns to be made up of fight count (So number of fights)
    df1
    .withColumn(
        "fight_count",
#F.count(*) counts ALL the rows within the partition.
        F.count("*").over(
#We create a window function, that creates a partition for each row in "f_1_name"
            Window.partitionBy("f_1_name")
        )
    )
#We select only what we want to display, reduce to one row with .distinct() and then order by most to least.
    .select("f_1_name", "fight_count")
    .distinct()
    .orderBy(F.desc("fight_count"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q8


# CELL ********************

# Q8  | Days since previous fight                           | `F.lag(...).over(w)` + `F.datediff`                                                                                         |

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

window = Window.partitionBy("f_1_name").orderBy("event_date")

display(
    df1
    .withColumn("previous_fight", F.lag("event_date").over(window))
    .withColumn("days_since_previous", F.datediff("event_date", "previous_fight"))
    .withColumn("fight_number", F.row_number().over(window.orderBy(F.desc("event_date"))))
    .filter(F.col("fight_number") == 1)
    .select("f_1_name", "event_date", "previous_fight", "days_since_previous")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q9

# CELL ********************

# Q9  | Only each fighter's 1st fight                       | `.withColumn(...)` then `.filter(...)`. No CTE needed: the chain *is* the CTE                                               |

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

#We make the window function that partitions into fighter names, and then orders into event dates
window = Window.partitionBy("f_1_name").orderBy("event_date")

#We put it into a data frame and make a column for fight number
display(
    df1
    .withColumn("fight_number", F.row_number().over(window))
    .filter(F.col("fight_number") == 1)
    .select("f_1_name", "event_date", "fight_url")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Q10

# CELL ********************

# Q10 | Anti-join `ufc_features` vs `full_data_silver_plus` | `.join(other, on=..., how="left_anti")`                                                                                     |

from pyspark.sql import functions as F

full_data = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

ufc_features = spark.read.parquet("Files/kaggle_file_load/2026-09-24/ufc_features.parquet")

display(
    ufc_features
    .join(
        full_data,
        "fight_url",
        "left_anti"
    )
    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")
display(df1)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
