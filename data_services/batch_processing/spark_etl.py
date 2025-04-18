# spark_etl.py
import logging
import os
import time
from datetime import datetime
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lit, sha2
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.utils import AnalysisException
from pyspark.dbutils import DBUtils

logger = logging.getLogger("SparkETL")

class SparkETLJob:
    def __init__(self, config: Dict):
        self.config = config
        self.spark = self._initialize_spark_session()
        self.dbutils = DBUtils(self.spark)
        self._validate_config()

    def _initialize_spark_session(self):
        return SparkSession.builder \
            .appName("AzeercBatchETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", self.config.get("shuffle_partitions", 200)) \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.maxExecutors", self.config.get("max_executors", 50)) \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
            .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate()

    def _validate_config(self):
        required_keys = {"input_paths", "output_table", "processing_date"}
        if not required_keys.issubset(self.config):
            raise ValueError(f"Missing required config keys: {required_keys - self.config.keys()}")

    def run(self):
        try:
            self._log_execution_start()
            raw_df = self._load_data()
            cleaned_df = self._clean_data(raw_df)
            transformed_df = self._apply_transformations(cleaned_df)
            self._validate_data_quality(transformed_df)
            self._write_output(transformed_df)
            self._log_execution_success()
        except Exception as e:
            self._handle_failure(e)
        finally:
            self.spark.stop()

    def _load_data(self):
        schema = StructType([
            StructField("event_id", StringType(), nullable=False),
            StructField("event_time", TimestampType(), nullable=True),
            StructField("user_id", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
            StructField("source_system", StringType(), nullable=True)
        ])

        return self.spark.read \
            .format(self.config.get("input_format", "parquet")) \
            .schema(schema) \
            .option("mergeSchema", "true") \
            .option("basePath", self.config.get("base_path", "/data/lake/bronze")) \
            .load(self.config["input_paths"])

    def _clean_data(self, df):
        return df.dropDuplicates(["event_id"]) \
            .withColumn("user_id", when(col("user_id").isNull(), "ANONYMOUS").otherwise(col("user_id"))) \
            .withColumn("value", when(col("value").isNull(), 0.0).otherwise(col("value"))) \
            .filter(col("event_time") >= lit(self.config["processing_date"])) \
            .dropna(subset=["event_id", "event_time"])

    def _apply_transformations(self, df):
        transform_rules = {
            "value_category": when(col("value") > 1000, "HIGH").otherwise("NORMAL"),
            "hashed_user_id": sha2(col("user_id"), 256),
            "time_bucket": (col("event_time").cast("long") / 300).cast("long") * 300
        }

        return df.transform(self._anonymize_pii) \
            .withColumn("processing_date", lit(self.config["processing_date"])) \
            .select("*", *[transform_rules[k].alias(k) for k in transform_rules])

    @staticmethod
    def _anonymize_pii(df):
        return df.withColumn("user_ip", udf(lambda x: x[:-3] + "***" if x else None, StringType())(col("user_ip")))

    def _validate_data_quality(self, df):
        dq_checks = {
            "row_count": df.count(),
            "null_user_ids": df.filter(col("user_id") == "ANONYMOUS").count(),
            "negative_values": df.filter(col("value") < 0).count()
        }

        self._write_dq_metrics(dq_checks)
        if dq_checks["row_count"] == 0:
            raise ValueError("Empty dataset after transformations")

    def _write_dq_metrics(self, metrics: Dict):
        metrics_df = self.spark.createDataFrame([{
            "execution_id": self.config["execution_id"],
            "metric_name": k,
            "metric_value": str(v),
            "timestamp": datetime.utcnow()
        } for k, v in metrics.items()])

        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("azeerc_etl.dq_metrics")

    def _write_output(self, df):
        df.write \
            .format("delta") \
            .partitionBy("processing_date", "source_system") \
            .option("replaceWhere", f"processing_date = '{self.config['processing_date']}'") \
            .option("maxRecordsPerFile", 1000000) \
            .option("compression", "zstd") \
            .mode("overwrite") \
            .saveAsTable(self.config["output_table"])

    def _log_execution_start(self):
        logger.info(f"Starting ETL job {self.config['execution_id']}")
        self.spark.sparkContext.setJobDescription(f"Azeerc ETL {self.config['processing_date']}")

    def _log_execution_success(self):
        self.dbutils.notebook.exit("SUCCESS")

    def _handle_failure(self, error):
        logger.error(f"Job failed: {str(error)}")
        self.dbutils.notebook.exit("FAILED")
        raise RuntimeError("ETL Job Failed") from error

if __name__ == "__main__":
    job_config = {
        "execution_id": os.getenv("EXECUTION_ID", f"etl-{datetime.utcnow().isoformat()}"),
        "input_paths": "/data/lake/bronze/events/dt=2023-08-01/*",
        "output_table": "azeerc_etl.processed_events",
        "processing_date": "2023-08-01",
        "shuffle_partitions": 200,
        "max_executors": 30
    }

    logging.basicConfig(level=logging.INFO)
    etl_job = SparkETLJob(job_config)
    etl_job.run()
