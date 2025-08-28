import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--user1_csv", required=True, help="HDFS path to User1 CSV")
    p.add_argument("--user2_csv", required=True, help="HDFS path to User2 CSV")
    p.add_argument("--output_base", required=True, help="HDFS base output dir")
    p.add_argument("--head_n", type=int, default=645, help="Rows to keep for user1 via head()")
    p.add_argument("--single_file", action="store_true",
                   help="If set, coalesce(1) before write to get a single part file.")
    return p.parse_args()

def build_spark():
    return (
        SparkSession.builder
        .appName("monkeytype-preprocessing")
        .getOrCreate()
    )

def to_datetime_from_ms(col_ms):
    return F.to_timestamp(F.from_unixtime((col_ms.cast("double") / F.lit(1000.0))))

def time_of_day_col(ts_col):
    hr = F.hour(ts_col)
    return (
        F.when((hr >= 5) & (hr < 12), F.lit("morning"))
         .when((hr >= 12) & (hr < 17), F.lit("afternoon"))
         .when((hr >= 17) & (hr < 22), F.lit("evening"))
         .otherwise(F.lit("night"))
    )

def split_charstats(col_str):
    arr = F.split(F.coalesce(col_str, F.lit("")), ";")
    def safe_int(idx):
        return F.coalesce(
            F.when(F.size(arr) >= F.lit(idx), F.element_at(arr, F.lit(idx)).cast("int")),
            F.lit(0)
        )
    return safe_int(1), safe_int(2), safe_int(3), safe_int(4)

def clean_user_df(spark, csv_path, user_id, head_n=None):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)   
        .csv(csv_path)
    )

    df = df.withColumn("mode2", F.col("mode2").cast("int"))
    df = df.withColumn("quoteLength", F.coalesce(F.col("quoteLength").cast("int"), F.lit(0)))
    df = df.filter(F.col("mode2") == F.lit(30))


    if head_n is not None and user_id == 1:
        rows = df.head(head_n)
        df = spark.createDataFrame(rows, schema=df.schema)

    df = df.withColumn("datetime", to_datetime_from_ms(F.col("timestamp")))
    df = df.withColumn("time_of_day", time_of_day_col(F.col("datetime")))

    c, ic, ex, mi = split_charstats(F.col("charStats"))
    df = (df
          .withColumn("correct_characters", c)
          .withColumn("incorrect_characters", ic)
          .withColumn("extra_characters", ex)
          .withColumn("missed_characters", mi))

    df = df.withColumn("user_id", F.lit(user_id).cast("int"))

    drop_cols = [
        "_id", "mode", "mode2", "quoteLength",
        "punctuation", "numbers", "language", "funbox",
        "difficulty", "lazyMode", "tags", "blindMode",
        "bailedOut", "isPb", "timestamp", "charStats"
    ]
    present_to_drop = [c for c in drop_cols if c in df.columns]
    df = df.drop(*present_to_drop)

    ordered = [
        "user_id", "datetime", "time_of_day",
        "wpm", "rawWpm", "acc", "consistency",
        "restartCount", "testDuration", "afkDuration", "incompleteTestSeconds",
        "correct_characters", "incorrect_characters", "extra_characters", "missed_characters",
    ]

    cols_final = [c for c in ordered if c in df.columns] + [c for c in df.columns if c not in ordered]
    df = df.select(*cols_final)

    return df

def write_parquet(df, out_dir, single_file=False):
    if single_file:
        df.coalesce(1).write.mode("overwrite").parquet(out_dir)
    else:
        df.write.mode("overwrite").parquet(out_dir)

def main():
    args = parse_args()
    spark = build_spark()

    u1 = clean_user_df(spark, args.user1_csv, user_id=1, head_n=args.head_n)
    u2 = clean_user_df(spark, args.user2_csv, user_id=2, head_n=None)

    out_u1 = f"{args.output_base}/filtered_user1.parquet"
    out_u2 = f"{args.output_base}/filtered_user2.parquet"
    write_parquet(u1, out_u1, single_file=args.single_file)
    write_parquet(u2, out_u2, single_file=args.single_file)

    combined = u1.unionByName(u2, allowMissingColumns=True)
    out_combined = f"{args.output_base}/combined_df.parquet"
    write_parquet(combined, out_combined, single_file=args.single_file)

    print("User1 count:", u1.count())
    print("User2 count:", u2.count())
    print("Combined count:", combined.count())
    print("Wrote:")
    print(" -", out_u1)
    print(" -", out_u2)
    print(" -", out_combined)

    spark.stop()

if __name__ == "__main__":
    main()
