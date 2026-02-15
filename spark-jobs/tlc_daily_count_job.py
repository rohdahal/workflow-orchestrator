from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
from datetime import datetime


def pickup_col(dataset: str) -> str:
    if dataset == "yellow":
        return "tpep_pickup_datetime"
    if dataset == "green":
        return "lpep_pickup_datetime"
    if dataset in ("fhv", "hvfhv"):
        return "pickup_datetime"
    raise ValueError(f"Unsupported dataset: {dataset}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True)
    p.add_argument("--year_month", required=True)
    p.add_argument("--s3_path", required=True)
    p.add_argument("--jdbc_url", required=True)
    p.add_argument("--jdbc_user", required=True)
    p.add_argument("--jdbc_password", required=True)
    args = p.parse_args()

    start = datetime.strptime(args.year_month + "-01", "%Y-%m-%d").date()
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)

    base = args.s3_path.rstrip("/")
    dataset = args.dataset
    filename = f"{dataset}_tripdata_{args.year_month}.parquet"
    if base.endswith(f"/{dataset}"):
        parquet_path = f"{base}/{filename}"
    else:
        parquet_path = f"{base}/{dataset}/{filename}"

    spark = SparkSession.builder.appName(f"TLC Daily Count {dataset} {args.year_month}").getOrCreate()
    try:
        trips = spark.read.parquet(parquet_path)
        ts_col = pickup_col(dataset)

        daily = (
            trips.withColumn("pickup_date", F.to_date(F.col(ts_col)))
            .filter((F.col("pickup_date") >= F.lit(str(start))) & (F.col("pickup_date") < F.lit(str(end))))
            .groupBy("pickup_date")
            .agg(F.count(F.lit(1)).cast("long").alias("trip_count"))
            .withColumn("dataset", F.lit(dataset))
            .select("dataset", "pickup_date", "trip_count")
        )

        (daily.write.mode("append")
         .format("jdbc")
         .option("url", args.jdbc_url)
         .option("dbtable", "tlc_daily_counts")
         .option("user", args.jdbc_user)
         .option("password", args.jdbc_password)
         .option("driver", "org.postgresql.Driver")
         .save())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
