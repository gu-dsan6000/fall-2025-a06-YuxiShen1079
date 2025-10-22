#!/usr/bin/env python3
import os, sys, argparse, logging
from collections import OrderedDict
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, regexp_extract, trim, lit, count as spark_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s")
logger = logging.getLogger(__name__)

LEVEL_REGEX = r"(?i)\b(INFO|WARN|ERROR|DEBUG)\b"

def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem1_cluster")

        # Cluster Configuration
        .master(master_url)  # Connect to Spark cluster

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark

def ensure_parent(p):
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def main():
    INPUT_PREFIX = "s3a://ys1079-assignment-spark-cluster-logs/data/"
    LOCAL_OUT_DIR = os.path.expanduser("~/output/")

    ap = argparse.ArgumentParser(description="Problem 1 — log level distribution")
    ap.add_argument("master_url", help="Spark master URL, e.g. spark://10.0.0.7:7077")
    ap.add_argument("--net-id", required=True, help="Your NetID, e.g., ys1079")
    ap.add_argument("--input", default=None,
                    help="Optional explicit input path (e.g., s3a://.../*.log). "
                         "If omitted, builds from --net-id.")
    ap.add_argument("--outdir", default=os.path.expanduser("~/spark-cluster"),
                    help="Local output directory on the master node")
    args = ap.parse_args()

    master_url = args.master_url

    # Default S3 input path, unless overridden
    input_path = args.input or f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/raw/application_*/*.log"

    counts_csv  = os.path.join(args.outdir, "problem1_counts.csv")
    sample_csv  = os.path.join(args.outdir, "problem1_sample.csv")
    summary_txt = os.path.join(args.outdir, "problem1_summary.txt")
    for p in (counts_csv, sample_csv, summary_txt):
        ensure_parent(p)

    spark = create_spark_session(master_url)
    logger.info("Reading logs from: %s", input_path)

    # Read raw logs as text
    df_raw = spark.read.text(input_path)

    # Parse fields; keep close to your original approach
    logs_parsed = df_raw.select(
        # basic timestamp at start of line if present
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', LEVEL_REGEX, 1).alias('log_level'),
        regexp_extract('value', r'(?:INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 1).alias('component'),
        col('value').alias('message')
    )

    # Keep rows where a level was found
    df_lv = logs_parsed.filter(col("log_level") != "")

    # Counts by level
    counts_df = df_lv.groupBy("log_level").agg(spark_count(lit(1)).alias("count"))
    by_level = {r["log_level"].upper(): int(r["count"]) for r in counts_df.collect()}

    # Preferred ordering, then any extras alphabetically
    preferred = ["INFO", "WARN", "ERROR", "DEBUG"]
    ordered = OrderedDict((lvl, by_level.get(lvl, 0)) for lvl in preferred if lvl in by_level)
    for k in sorted(by_level.keys()):
        if k not in ordered:
            ordered[k] = by_level[k]

    # Write counts CSV (small → fine to collect into pandas)
    pd.DataFrame([{"log_level": k, "count": v} for k, v in ordered.items()]) \
      .to_csv(counts_csv, index=False)

    # Random sample of 10 (convert to pandas safely because it's tiny)
    (df_lv
        .select(col("message").alias("log_entry"), "log_level")
        .orderBy(rand())
        .limit(10)
        .toPandas()
        .to_csv(sample_csv, index=False))

    # Summary text
    total_lines = df_raw.count()
    total_with_level = df_lv.count()
    unique_levels = len(ordered)
    grand = max(total_with_level, 1)

    lines = []
    lines.append(f"Input path: {input_path}")
    lines.append(f"Total log lines processed: {total_lines:,}")
    lines.append(f"Total lines with log levels: {total_with_level:,}")
    lines.append(f"Unique log levels found: {unique_levels}")
    lines.append("")
    lines.append("Log level distribution:")
    for lvl, cnt in ordered.items():
        pct = (cnt / grand) * 100.0
        lines.append(f"{lvl:<6}: {cnt:>8,} ({pct:6.2f}%)")
    lines.append("")

    os.makedirs(args.outdir, exist_ok=True)
    with open(summary_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info("Wrote:\n  %s\n  %s\n  %s", counts_csv, sample_csv, summary_txt)
    spark.stop()

if __name__ == "__main__":
    main()