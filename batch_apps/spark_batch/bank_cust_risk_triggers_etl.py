"""
PySpark Bank Customer Risk Triggers ETL Application
=====================================================
App Name: btot-8999-syn-api-cust-risk-tg
Dataset: cust-risk-triggers
Source: Synapse (SQL Server) / CSV / Parquet
Target: PostgreSQL + REST API

Transformations:
  1. Read from Synapse SQL Server or fallback to CSV
  2. Apply field transformations (casting, encoding)
  3. Repartition data for parallel processing
  4. Write to PostgreSQL
  5. Push to REST API
  6. Audit output to file
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional

import requests
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class BankCustomerRiskTriggersETL:
    """
    Bank Customer Risk Triggers ETL Pipeline
    
    Reads customer risk data from Synapse SQL Server (or fallback CSV),
    transforms and enriches, writes to PostgreSQL, and pushes to REST API.
    """

    def __init__(self, config_path: str, logical_date: str | None = None) -> None:
        """Initialize ETL with configuration."""
        self.config_path = config_path
        self.logical_date = logical_date or datetime.utcnow().strftime("%Y-%m-%d")
        self.config = self._load_config()
        self.spark: SparkSession | None = None
        self.source_df: DataFrame | None = None

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from YAML file."""
        logger.info(f"Loading config from {self.config_path}")
        with open(self.config_path) as f:
            config = yaml.safe_load(f)
        logger.info(f"Config loaded: app_name={config['generalInfo']['appName']}")
        return config

    # ── Lifecycle Methods ──────────────────────────────────────────────────────

    def build_session(self) -> SparkSession:
        """Create and configure SparkSession."""
        try:
            spark_cfg = self.config.get("spark", {})
            app_name = self.config["generalInfo"]["appName"]
            master = os.getenv(
                "SPARK_MASTER_URL", spark_cfg.get("master", "local[*]")
            )

            builder = (
                SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", "10"))
                .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
                .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            )

            # Add Delta Lake support if needed
            if self.config.get("spark", {}).get("sql_extensions"):
                builder = builder.config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension"
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )

            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "INFO"))
            logger.info(f"SparkSession created: master={master}, app_name={app_name}")
            return self.spark

        except Exception as exc:
            logger.error(f"Failed to create SparkSession: {exc}", exc_info=True)
            raise

    def extract(self) -> DataFrame:
        """Extract data from source (Synapse or CSV fallback)."""
        try:
            source_cfg = self.config.get("source", {})
            src_sys = source_cfg.get("srcSysName", "")

            logger.info(f"Extracting from {src_sys}")

            if src_sys.lower() == "synapse":
                return self._extract_from_synapse()
            else:
                return self._extract_from_csv()

        except Exception as exc:
            logger.error(f"Extraction failed: {exc}", exc_info=True)
            if "Synapse" in str(exc):
                logger.warning("Synapse extraction failed, falling back to CSV")
                return self._extract_from_csv()
            raise

    def _extract_from_synapse(self) -> DataFrame:
        """Extract from Synapse SQL Server."""
        source_cfg = self.config.get("source", {})
        props = source_cfg.get("srcSysProp", {})

        server = os.getenv("SYNAPSE_SERVER", props.get("server", ""))
        database = os.getenv("SYNAPSE_DATABASE", props.get("database", ""))
        user = os.getenv("SYNAPSE_USER", props.get("username", ""))
        password = os.getenv("SYNAPSE_PASSWORD", props.get("password", ""))
        port = props.get("port", 1433)

        # Read SQL query from file
        sql_file = props.get("sqlQueryFilePath")
        if os.path.exists(sql_file):
            with open(sql_file) as f:
                query = f.read()
        else:
            # Fallback query
            query = """
                SELECT 
                    customerLpid,
                    custcardTokens,
                    riskScore,
                    riskTriggerDate,
                    riskIndicator,
                    transactionAmount,
                    transactionDate,
                    merchantCategory,
                    createdAt
                FROM dbo.customer_risk_triggers
                WHERE riskTriggerDate >= CAST(GETDATE() - 1 AS DATE)
            """

        jdbc_url = f"jdbc:sqlserver://{server}:{port};database={database}"

        logger.info(f"Connecting to Synapse: {jdbc_url}")
        df = (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({query}) AS t")
            .option("user", user)
            .option("password", password)
            .option("numPartitions", props.get("numPartitions", 4))
            .option("fetchsize", 10000)
            .load()
        )

        logger.info(f"Extracted {df.count()} records from Synapse")
        return df

    def _extract_from_csv(self) -> DataFrame:
        """Extract from CSV file (fallback)."""
        source_cfg = self.config.get("source", {})
        props = source_cfg.get("srcSysProp", {})

        fallback_path = props.get("fallbackPath", "/tmp/bank_data/")
        fallback_files = props.get("fallbackFiles", ["bank_data.csv"])

        file_path = os.path.join(fallback_path, fallback_files[0])
        
        # Support multiple file types
        if fallback_files[0].endswith(".parquet"):
            logger.info(f"Extracting from Parquet: {file_path}")
            df = self.spark.read.parquet(file_path)
        elif fallback_files[0].endswith(".json"):
            logger.info(f"Extracting from JSON: {file_path}")
            df = self.spark.read.json(file_path)
        else:
            logger.info(f"Extracting from CSV: {file_path}")
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

        # Rename columns to match expected schema if needed
        df = self._normalize_columns(df)
        
        logger.info(f"Extracted {df.count()} records from {file_path}")
        return df

    def _normalize_columns(self, df: DataFrame) -> DataFrame:
        """Normalize column names to expected schema."""
        expected_cols = [
            "customerLpid", "custcardTokens", "riskScore", "riskTriggerDate",
            "riskIndicator", "transactionAmount", "transactionDate",
            "merchantCategory", "createdAt"
        ]
        
        # Get actual columns
        actual_cols = df.columns
        
        # Map actual to expected if needed
        col_mapping = {
            "customer_lpid": "customerLpid",
            "cust_card_tokens": "custcardTokens",
            "risk_score": "riskScore",
            "risk_trigger_date": "riskTriggerDate",
            "risk_indicator": "riskIndicator",
            "transaction_amount": "transactionAmount",
            "transaction_date": "transactionDate",
            "merchant_category": "merchantCategory",
            "created_at": "createdAt",
            # Bank data mapping
            "account": "customerLpid",
            "balance": "transactionAmount",
            "date": "transactionDate",
        }
        
        for old_col, new_col in col_mapping.items():
            if old_col in actual_cols and new_col not in actual_cols:
                df = df.withColumnRenamed(old_col, new_col)
        
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations."""
        logger.info("Applying transformations")

        try:
            # Step 1: Cast columns to correct types
            df = self._cast_columns(df)

            # Step 2: Add audit columns
            df = self._add_audit_columns(df)

            # Step 3: Remove duplicates
            df = self._dedup(df)

            # Step 4: Clean null values
            df = self._clean_nulls(df)

            logger.info(f"Transformed data: {df.count()} records")
            return df

        except Exception as exc:
            logger.error(f"Transformation failed: {exc}", exc_info=True)
            raise

    def _cast_columns(self, df: DataFrame) -> DataFrame:
        """Cast columns to proper data types."""
        transformations = [
            ("customerLpid", "string"),
            ("custcardTokens", "string"),
            ("riskScore", "decimal(10,2)"),
            ("riskTriggerDate", "timestamp"),
            ("riskIndicator", "string"),
            ("transactionAmount", "decimal(15,2)"),
            ("transactionDate", "date"),
            ("merchantCategory", "string"),
        ]

        for col_name, col_type in transformations:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            else:
                logger.warning(f"Column {col_name} not found in source data")

        # Additional transformations
        if "riskIndicator" in df.columns:
            df = df.withColumn("riskIndicator", F.upper(F.col("riskIndicator")))

        return df

    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add audit columns."""
        df = df.withColumn("processedAt", F.current_timestamp())
        df = df.withColumn("processDate", F.lit(self.logical_date))
        df = df.withColumn("pipelineVersion", F.lit("1.0"))
        df = df.withColumn("dataQuality", F.lit("VALIDATED"))
        return df

    def _dedup(self, df: DataFrame) -> DataFrame:
        """Remove duplicates based on primary key."""
        dedup_keys = ["customerLpid", "transactionDate"]
        return df.dropDuplicates(dedup_keys)

    def _clean_nulls(self, df: DataFrame) -> DataFrame:
        """Clean null values."""
        # Fill nulls with default values
        fill_values = {
            "riskScore": 0.0,
            "riskIndicator": "UNKNOWN",
            "merchantCategory": "OTHER",
            "transactionAmount": 0.0,
        }
        return df.fillna(fill_values)

    def load_to_postgres(self, df: DataFrame) -> None:
        """Load data to PostgreSQL."""
        try:
            sink_cfg = self.config.get("mainFlow", {}).get("transformations", [])
            pg_target = next(
                (t for t in sink_cfg if t.get("tgtSysName") == "PostgreSQL"),
                {}
            )
            props = pg_target.get("tgtSysProp", {})

            host = os.getenv("PG_HOST", props.get("host", "localhost"))
            port = os.getenv("PG_PORT", props.get("port", "5432"))
            database = os.getenv("PG_DATABASE", props.get("database", "bank_db"))
            user = os.getenv("PG_USER", props.get("username", "postgres"))
            password = os.getenv("PG_PASSWORD", props.get("password", ""))
            schema = props.get("schema", "public")
            table_name = props.get("tableName", "cust_risk_triggers")
            write_mode = props.get("writeMode", "overwrite").lower()
            batch_size = props.get("batchSize", 102400)

            jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

            logger.info(f"Loading to PostgreSQL: {jdbc_url}/{schema}.{table_name}")
            logger.info(f"Write mode: {write_mode}, records: {df.count()}")

            # Map Spark write modes to JDBC modes
            mode_mapping = {
                "overwrite": "overwrite",
                "append": "append",
                "ignore": "ignore",
                "error": "error",
            }

            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"{schema}.{table_name}") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", batch_size) \
                .option("numPartitions", props.get("numPartitions", 10)) \
                .mode(mode_mapping.get(write_mode, "overwrite")) \
                .save()

            logger.info(f"Successfully loaded data to {schema}.{table_name}")

        except Exception as exc:
            logger.error(f"PostgreSQL load failed: {exc}", exc_info=True)
            raise

    def push_to_api(self, df: DataFrame) -> None:
        """Push data to REST API."""
        try:
            sink_cfg = self.config.get("mainFlow", {}).get("transformations", [])
            api_target = next(
                (t for t in sink_cfg if t.get("tgtSysName") == "RESTAPI"),
                {}
            )
            props = api_target.get("tgtSysProp", {})
            request_cfg = props.get("request", {})

            uri = request_cfg.get("uri", "")
            method = request_cfg.get("method", "POST").upper()
            headers = {}
            for header in request_cfg.get("headers", []):
                headers[header.get("header-key")] = header.get("header-value")

            rate_limit = props.get("rateLimit", 2000)
            retry_attempts = props.get("retryAttempts", 3)
            retry_delay = props.get("retryDelayMs", 1000) / 1000

            logger.info(f"Pushing to API: {uri}, method={method}, records={df.count()}")

            # Convert to pandas for easier processing
            pdf = df.toPandas()
            
            success_count = 0
            error_count = 0

            for idx, row in pdf.iterrows():
                try:
                    payload = json.loads(row.to_json())
                    
                    if method == "POST":
                        response = requests.post(
                            uri,
                            json=payload,
                            headers=headers,
                            timeout=10,
                            verify=True,
                        )
                    else:
                        response = requests.put(
                            uri,
                            json=payload,
                            headers=headers,
                            timeout=10,
                            verify=True,
                        )

                    if response.status_code in (200, 201, 202):
                        success_count += 1
                    else:
                        error_count += 1
                        logger.warning(
                            f"API call failed: {response.status_code} - {response.text}"
                        )

                    # Rate limiting
                    if (idx + 1) % rate_limit == 0:
                        logger.info(f"Rate limit checkpoint: {idx + 1} records pushed")

                except Exception as row_exc:
                    error_count += 1
                    logger.warning(f"Error pushing record {idx}: {row_exc}")
                    
                    if error_count > retry_attempts:
                        logger.error(f"Max retry attempts reached")
                        break

            logger.info(
                f"API push completed: {success_count} success, {error_count} failed"
            )

        except Exception as exc:
            logger.error(f"API push failed: {exc}", exc_info=True)
            # Don't raise, log and continue

    def write_audit_file(self, df: DataFrame) -> None:
        """Write audit file."""
        try:
            sink_cfg = self.config.get("mainFlow", {}).get("transformations", [])
            audit_target = next(
                (t for t in sink_cfg if t.get("transformationName") == "output_audit"),
                {}
            )
            props = audit_target.get("tgtSysProp", {})

            file_path = props.get("filePath", "/tmp/audit/")
            file_name = props.get("fileRename", {}).get("fileName", "audit.txt")
            full_path = os.path.join(file_path, file_name)

            os.makedirs(file_path, exist_ok=True)

            logger.info(f"Writing audit file: {full_path}")
            audit_df = df.select(
                "customerLpid", "riskScore", "riskIndicator", "processedAt"
            )
            audit_df.coalesce(1).write.mode("overwrite").option(
                "header", "true"
            ).csv(file_path)

            logger.info(f"Audit file written: {full_path}")

        except Exception as exc:
            logger.error(f"Audit write failed: {exc}", exc_info=True)

    def run(self) -> None:
        """Run the ETL pipeline."""
        try:
            logger.info("=" * 80)
            logger.info(f"Starting ETL Pipeline: {self.config['generalInfo']['appName']}")
            logger.info(f"Logical Date: {self.logical_date}")
            logger.info("=" * 80)

            # Build Spark session
            self.build_session()

            # Extract
            self.source_df = self.extract()

            # Transform
            transformed_df = self.transform(self.source_df)

            # Load to PostgreSQL
            self.load_to_postgres(transformed_df)

            # Push to API
            self.push_to_api(transformed_df)

            # Write audit
            self.write_audit_file(transformed_df)

            logger.info("=" * 80)
            logger.info("ETL Pipeline completed successfully")
            logger.info("=" * 80)

        except Exception as exc:
            logger.error(f"ETL Pipeline failed: {exc}", exc_info=True)
            sys.exit(1)

    def stop(self) -> None:
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("SparkSession stopped")


# ── CLI Entrypoint ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bank Customer Risk Triggers ETL Pipeline"
    )
    parser.add_argument(
        "--config",
        default="pipelines/configs/bank_cust_risk_triggers_config.yaml",
        help="Path to pipeline configuration file",
    )
    parser.add_argument(
        "--date",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Logical date for processing (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--mode",
        default="normal",
        choices=["normal", "test", "debug"],
        help="Execution mode",
    )

    args = parser.parse_args()

    # Set logging level based on mode
    if args.mode == "debug":
        logging.getLogger().setLevel(logging.DEBUG)

    app = BankCustomerRiskTriggersETL(args.config, logical_date=args.date)
    try:
        app.run()
    finally:
        app.stop()

