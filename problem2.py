#!/usr/bin/env python3
import os, sys, argparse, logging
from collections import OrderedDict
import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, rand, regexp_extract, trim, lit, count as spark_count, to_timestamp, coalesce, input_file_name, count_distinct


import re
import shutil
import tempfile
from pathlib import Path 

import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s")
logger = logging.getLogger(__name__)

OUT_DIR = os.path.expanduser("~/spark-cluster")
TIMELINE_CSV = OUT_DIR / "problem2_timeline.csv"
CLUSTER_SUMMARY_CSV = OUT_DIR / "problem2_cluster_summary.csv"
STATS_TXT = OUT_DIR / "problem2_stats.txt"
BAR_PNG = OUT_DIR / "problem2_bar_chart.png"
DENSITY_PNG = OUT_DIR / "problem2_density_plot.png"

APP_DIR_RE = r"(application_(\d+)_(\d+))"  # application_<cluster_id>_<app_number>
TS_ISO_RE = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"  # 2025-10-22 10:04:41
TS_LEGACY_RE = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"  # 25/10/22 10:04:41

def ensure_outdir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def create_spark_session(master_url):
    """Create a Spark session optimized for cluster execution."""

    spark = (
        SparkSession.builder
        .appName("Problem2_cluster")

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
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,"
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("fs.s3a.requester.pays.enabled", "true") 
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

def save_single_csv_spark(df, path: Path):
    """Write a Spark DataFrame as a single CSV with header."""

    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        is_empty = df.rdd.isEmpty()
    except Exception:
        is_empty = df.limit(1).count() == 0

    if is_empty:
        import pandas as pd
        pd.DataFrame(columns=df.columns).to_csv(path, index=False)
        return
    
    # tmp = Path(tempfile.mkdtemp(prefix="p2csv_"))
    # (df.coalesce(1)
    #    .write.mode("overwrite")
    #    .option("header", True)
    #    .csv(str(tmp)))
    # part = next(tmp.glob("part-*.csv"))
    # path.parent.mkdir(parents=True, exist_ok=True)
    # shutil.move(str(part), str(path))

    tmp = Path(tempfile.mkdtemp(prefix="p2csv_"))
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(str(tmp)))
    parts = list(tmp.glob("part-*.csv"))
    if parts:
        shutil.move(str(parts[0]), str(path))


    shutil.rmtree(tmp, ignore_errors=True)

def run_spark(master_url: str, net_id: str, input_override: str | None):
    """
    Reads all *.log under
      s3a://<NET-ID>-assignment-spark-cluster-logs/data/*/*.log
    and computes application start/end times by application directory.
    """
    spark = create_spark_session(master_url)  
    input_path = input_override or f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/container_*.log"

    # Read text logs with file path
    df = (spark.read.text(input_path)
                .withColumn("path", F.input_file_name()))
    if df.limit(1).count() == 0:
        raise SystemExit(
            f"No files matched input_path:\n  {input_path}\n\n"
            "Try this on the master:\n"
            "  hadoop fs -D fs.s3a.requester.pays.enabled=true "
            f"-ls 's3a://{net_id}-assignment-spark-cluster-logs/data/application_*/container_*.log' | head"
        )
    # Extract application id pieces from the path
    df = (df.withColumn("application_id", F.regexp_extract("path", APP_DIR_RE, 1))
            .withColumn("cluster_id",     F.regexp_extract("path", APP_DIR_RE, 2))
            .withColumn("app_number",     F.regexp_extract("path", APP_DIR_RE, 3).cast(IntegerType())))

    # Parse timestamps (ISO and legacy)

    ts_iso    = F.regexp_extract(F.col("value"), TS_ISO_RE, 1)
    ts_legacy = F.regexp_extract(F.col("value"), TS_LEGACY_RE, 1)

    # Tolerant parsing: empty/non-matching → NULL, not error
    col_ts = F.coalesce(
        F.try_to_timestamp(ts_iso,    F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(ts_legacy, F.lit("yy/MM/dd HH:mm:ss")),
    ).alias("ts")

    df = df.withColumn("ts", col_ts)

    # Application min/max timestamps (start/end)
    apps = (df.where(F.col("application_id") != "")
            .where(F.col("ts").isNotNull())
            .groupBy("cluster_id", "application_id", "app_number")
            .agg(F.min("ts").alias("start_time"),
                F.max("ts").alias("end_time"))
            .orderBy(F.col("cluster_id").asc(), F.col("app_number").asc()))


    save_single_csv_spark(apps, TIMELINE_CSV)

    # Cluster summary
    cluster_summary = (apps.groupBy("cluster_id")
                            .agg(F.count("*").alias("num_applications"),
                                 F.min("start_time").alias("cluster_first_app"),
                                 F.max("end_time").alias("cluster_last_app"))
                            .orderBy(F.col("num_applications").desc(),
                                     F.col("cluster_id").asc()))
    save_single_csv_spark(cluster_summary, CLUSTER_SUMMARY_CSV)

    # Return tiny pandas frames for plotting/stats
    return (apps.toPandas(), cluster_summary.toPandas())


def write_stats_and_plots(timeline_pd: pd.DataFrame, summary_pd: pd.DataFrame):
    ensure_outdir()

    # Stats
    total_clusters = int(summary_pd["cluster_id"].nunique())
    total_apps = int(timeline_pd.shape[0])
    avg_apps = float(summary_pd["num_applications"].mean()) if not summary_pd.empty else 0.0

    top_lines = []
    top_lines.append(f"Total unique clusters: {total_clusters}")
    top_lines.append(f"Total applications: {total_apps}")
    top_lines.append(f"Average applications per cluster: {avg_apps:.2f}")
    top_lines.append("")
    top_lines.append("Most heavily used clusters:")
    for _, r in summary_pd.nlargest(5, "num_applications").iterrows():
        top_lines.append(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications")
    STATS_TXT.write_text("\n".join(top_lines), encoding="utf-8")

    # Duration seconds for each app
    # Ensure datetime dtype
    tl = timeline_pd.copy()
    tl["start_time"] = pd.to_datetime(tl["start_time"])
    tl["end_time"] = pd.to_datetime(tl["end_time"])
    tl["duration_sec"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds()

    # ---- Bar chart: applications per cluster
    plt.figure(figsize=(10, 5))
    ax = sns.barplot(
        data=summary_pd.sort_values("num_applications", ascending=False),
        x="cluster_id", y="num_applications"
    )
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f"{int(height)}", (p.get_x() + p.get_width()/2., height),
                    ha='center', va='bottom', xytext=(0, 3), textcoords='offset points', fontsize=9)
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Applications")
    ax.set_title("Applications per Cluster")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(BAR_PNG, dpi=150)
    plt.close()

    # ---- Density for the largest cluster
    if not summary_pd.empty:
        largest_cluster = str(summary_pd.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"])
        tl_big = tl[tl["cluster_id"] == largest_cluster].copy()

        plt.figure(figsize=(8, 5))
        ax = sns.histplot(tl_big["duration_sec"], bins=40, kde=True)
        ax.set_xscale("log")  # skewed durations
        ax.set_xlabel("Job duration (seconds, log scale)")
        ax.set_ylabel("Count")
        ax.set_title(f"Duration distribution — Cluster {largest_cluster} (n={len(tl_big)})")
        plt.tight_layout()
        plt.savefig(DENSITY_PNG, dpi=150)
        plt.close()


def main():
    parser = argparse.ArgumentParser(description="Problem 2 — Cluster Usage Analysis")
    parser.add_argument("master_url", nargs="?", help="Spark master URL, e.g., spark://10.0.0.7:7077")
    parser.add_argument("--net-id", help="Your NetID (for default S3 path)")
    parser.add_argument("--input", default=None,
                        help="Optional explicit input glob (e.g., s3a://bucket/data/*/*.log)")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark; read existing CSVs and only regenerate stats/plots.")
    args = parser.parse_args()

    ensure_outdir()

    if args.skip_spark:
        # Fast path: just reload existing CSVs and regenerate outputs
        if not TIMELINE_CSV.exists() or not CLUSTER_SUMMARY_CSV.exists():
            raise SystemExit(
                "Cannot --skip-spark because CSVs are missing.\n"
            )
        timeline_pd = pd.read_csv(TIMELINE_CSV)
        summary_pd = pd.read_csv(CLUSTER_SUMMARY_CSV)
        write_stats_and_plots(timeline_pd, summary_pd)
        print(f"Rebuilt plots/stats from existing CSVs:\n - {BAR_PNG}\n - {DENSITY_PNG}\n - {STATS_TXT}")
        return

    # Full Spark path
    if not args.master_url or not (args.net_id or args.input):
        raise SystemExit(
            "For full Spark run, provide master_url and either --net-id (recommended) or --input."
        )

    timeline_pd, summary_pd = run_spark(args.master_url, args.net_id, args.input)
    # Also write pandas versions of CSVs (they are identical to Spark ones but harmless)
    # Ensures header/order are stable
    timeline_pd.to_csv(TIMELINE_CSV, index=False)
    summary_pd.to_csv(CLUSTER_SUMMARY_CSV, index=False)

    write_stats_and_plots(timeline_pd, summary_pd)

    print("Wrote outputs:")
    print(f"  {TIMELINE_CSV}")
    print(f"  {CLUSTER_SUMMARY_CSV}")
    print(f"  {STATS_TXT}")
    print(f"  {BAR_PNG}")
    print(f"  {DENSITY_PNG}")

if __name__ == "__main__":
    main()
