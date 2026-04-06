import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["S3_BUCKET"]
BRONZE = f"s3://{BUCKET}/covid/bronze"
SILVER = f"s3://{BUCKET}/covid/silver"

upper_trim = lambda c: F.upper(F.trim(F.col(c)))

# -----------------------------
# Lookup: state code <-> state name
# -----------------------------
states = (
    spark.read.option("header", True)
    .csv(f"{BRONZE}/static/states_abv.csv")
    .select(
        upper_trim("Abbreviation").alias("state_code"),
        F.initcap(F.trim(F.col("State"))).alias("state_name")
    )
)

# -----------------------------
# CASES: NYT us_states.csv
# expected columns: date,state,fips,cases,deaths
# -----------------------------
cases_raw = spark.read.option("header", True).csv(f"{BRONZE}/nytimes/us_states.csv")

cases_std = (
    cases_raw
    .withColumn("full_date", F.to_date(F.col("date")))
    .withColumn("state_name_raw", F.initcap(F.trim(F.col("state"))))
    .join(states, states.state_name == F.col("state_name_raw"), "left")
    .withColumn("cases_cum", F.col("cases").cast("long"))
    .withColumn("deaths_cum", F.col("deaths").cast("long"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .select(
        "full_date",
        "state_code",
        "state_name",
        "cases_cum",
        "deaths_cum",
        "year",
        "month",
        "day"
    )
    .dropna(subset=["full_date", "state_code"])
)

(
    cases_std.write.mode("overwrite")
    .partitionBy("state_code", "year", "month", "day")
    .parquet(f"{SILVER}/cases_standardized")
)

# -----------------------------
# TESTING: states_daily.csv
# expected columns include date, state, positive, negative, totalTestResults
# -----------------------------
tests_raw = spark.read.option("header", True).csv(f"{BRONZE}/covid_tracking/states_daily.csv")

tests_std = (
    tests_raw
    .withColumn("full_date", F.to_date(F.col("date").cast("string"), "yyyyMMdd"))
    .withColumn("state_code", upper_trim("state"))
    .join(states.select("state_code", "state_name"), "state_code", "left")
    .withColumn("tests_total_cum", F.col("totalTestResults").cast("long"))
    .withColumn("tests_pos_cum", F.col("positive").cast("long"))
    .withColumn("tests_neg_cum", F.col("negative").cast("long"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .select(
        "full_date",
        "state_code",
        "state_name",
        "tests_total_cum",
        "tests_pos_cum",
        "tests_neg_cum",
        "year",
        "month",
        "day"
    )
    .dropna(subset=["full_date", "state_code"])
)

(
    tests_std.write.mode("overwrite")
    .partitionBy("state_code", "year", "month", "day")
    .parquet(f"{SILVER}/testing_standardized")
)

print("Silver complete.")
job.commit()
