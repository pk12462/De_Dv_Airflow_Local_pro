package com.bank.etl.cust_risk_triggers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.{Failure, Success, Try}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.sys.process._
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
  * Bank Customer Risk Triggers ETL Application
  *
  * App: btot-8999-syn-api-cust-risk-tg
  * Dataset: cust-risk-triggers
  *
  * Scala version of the customer risk triggers ETL pipeline
  * Source: Synapse (SQL Server) / CSV / Parquet
  * Target: PostgreSQL + REST API
  */

object BankCustomerRiskTriggersETL extends App {

  private val logger = LoggerFactory.getLogger(getClass)
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  case class CommandLineArgs(
    configPath: String = "pipelines/configs/bank_cust_risk_triggers_config.yaml",
    logicalDate: String = LocalDate.now().format(dateFormatter),
    mode: String = "normal"
  )

  object CommandLineParser {
    def parse(args: Array[String]): CommandLineArgs = {
      var config = CommandLineArgs()
      var index = 0

      while (index < args.length) {
        args(index) match {
          case "--config" =>
            index += 1
            if (index < args.length) config = config.copy(configPath = args(index))
          case "--date" =>
            index += 1
            if (index < args.length) config = config.copy(logicalDate = args(index))
          case "--mode" =>
            index += 1
            if (index < args.length) config = config.copy(mode = args(index))
          case _ => logger.warn(s"Unknown argument: ${args(index)}")
        }
        index += 1
      }
      config
    }
  }

  // Parse command line arguments
  val cmdArgs = CommandLineParser.parse(args)

  // Load configuration
  val config = ConfigFactory.load()

  // Initialize ETL pipeline
  val etl = new BankCustomerRiskTriggersETLPipeline(
    configPath = cmdArgs.configPath,
    logicalDate = cmdArgs.logicalDate,
    executionMode = cmdArgs.mode
  )

  // Run the pipeline
  try {
    logger.info("=" * 80)
    logger.info(s"Starting Bank Customer Risk Triggers ETL Pipeline")
    logger.info(s"Logical Date: ${cmdArgs.logicalDate}")
    logger.info(s"Execution Mode: ${cmdArgs.mode}")
    logger.info("=" * 80)

    etl.run()

    logger.info("=" * 80)
    logger.info("ETL Pipeline completed successfully")
    logger.info("=" * 80)
  } catch {
    case exc: Exception =>
      logger.error("ETL Pipeline failed", exc)
      sys.exit(1)
  } finally {
    etl.stop()
  }
}

/**
  * Main ETL Pipeline Implementation
  */
class BankCustomerRiskTriggersETLPipeline(
  configPath: String,
  logicalDate: String,
  executionMode: String
) {

  private val logger = LoggerFactory.getLogger(getClass)
  private var spark: SparkSession = _
  private var sourceDF: DataFrame = _

  /**
    * Build Spark Session with proper configurations
    */
  def buildSession(): SparkSession = {
    try {
      val appName = "btot-8999-syn-api-cust-risk-tg"
      val master = sys.env.getOrElse("SPARK_MASTER_URL", "local[*]")

      val sparkSession = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.executor.memory", sys.env.getOrElse("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.driver.memory", sys.env.getOrElse("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

      sparkSession.sparkContext.setLogLevel("INFO")
      this.spark = sparkSession

      logger.info(s"SparkSession created: master=$master, appName=$appName")
      sparkSession
    } catch {
      case exc: Exception =>
        logger.error("Failed to create SparkSession", exc)
        throw exc
    }
  }

  /**
    * Extract data from source systems
    */
  def extract(): DataFrame = {
    try {
      logger.info("Starting data extraction")
      // For now, extract from local CSV (Synapse connection would require JDBC drivers)
      extractFromCSV()
    } catch {
      case exc: Exception =>
        logger.error("Extraction failed", exc)
        throw exc
    }
  }

  /**
    * Extract from CSV file
    */
  private def extractFromCSV(): DataFrame = {
    val csvPath = sys.env.getOrElse("CSV_SOURCE_PATH", "bank_data.csv")
    logger.info(s"Extracting from CSV: $csvPath")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    logger.info(s"Extracted ${df.count()} records from CSV")
    normalizeColumns(df)
  }

  /**
    * Normalize column names
    */
  private def normalizeColumns(df: DataFrame): DataFrame = {
    val colMappings = Map(
      "customer_lpid" -> "customerLpid",
      "cust_card_tokens" -> "custcardTokens",
      "risk_score" -> "riskScore",
      "risk_trigger_date" -> "riskTriggerDate",
      "risk_indicator" -> "riskIndicator",
      "transaction_amount" -> "transactionAmount",
      "transaction_date" -> "transactionDate",
      "merchant_category" -> "merchantCategory",
      "created_at" -> "createdAt",
      // Bank data mappings
      "account" -> "customerLpid",
      "balance" -> "transactionAmount",
      "date" -> "transactionDate"
    )

    var result = df
    for ((oldCol, newCol) <- colMappings) {
      if (df.columns.contains(oldCol) && !df.columns.contains(newCol)) {
        result = result.withColumnRenamed(oldCol, newCol)
      }
    }
    result
  }

  /**
    * Apply transformations
    */
  def transform(df: DataFrame): DataFrame = {
    try {
      logger.info("Applying transformations")

      var transformed = df

      // Cast columns to proper types
      transformed = castColumns(transformed)

      // Add audit columns
      transformed = addAuditColumns(transformed)

      // Remove duplicates
      transformed = transformed.dropDuplicates(Array("customerLpid", "transactionDate"))

      // Clean null values
      transformed = cleanNulls(transformed)

      logger.info(s"Transformation completed: ${transformed.count()} records")
      transformed
    } catch {
      case exc: Exception =>
        logger.error("Transformation failed", exc)
        throw exc
    }
  }

  /**
    * Cast columns to proper types
    */
  private def castColumns(df: DataFrame): DataFrame = {
    var result = df

    val castMappings = Map(
      "customerLpid" -> "string",
      "custcardTokens" -> "string",
      "riskScore" -> "decimal(10,2)",
      "riskTriggerDate" -> "timestamp",
      "riskIndicator" -> "string",
      "transactionAmount" -> "decimal(15,2)",
      "transactionDate" -> "date",
      "merchantCategory" -> "string"
    )

    for ((colName, colType) <- castMappings) {
      if (df.columns.contains(colName)) {
        result = result.withColumn(colName, col(colName).cast(colType))
      } else {
        logger.warn(s"Column $colName not found in source data")
      }
    }

    // Additional transformations
    if (df.columns.contains("riskIndicator")) {
      result = result.withColumn("riskIndicator", upper(col("riskIndicator")))
    }

    result
  }

  /**
    * Add audit columns
    */
  private def addAuditColumns(df: DataFrame): DataFrame = {
    df
      .withColumn("processedAt", current_timestamp())
      .withColumn("processDate", lit(logicalDate))
      .withColumn("pipelineVersion", lit("1.0"))
      .withColumn("dataQuality", lit("VALIDATED"))
  }

  /**
    * Clean null values
    */
  private def cleanNulls(df: DataFrame): DataFrame = {
    df
      .na.fill(0.0, Array("riskScore", "transactionAmount"))
      .na.fill("UNKNOWN", Array("riskIndicator"))
      .na.fill("OTHER", Array("merchantCategory"))
  }

  /**
    * Load data to PostgreSQL
    */
  def loadToPostgres(df: DataFrame): Unit = {
    try {
      val pgHost = sys.env.getOrElse("PG_HOST", "localhost")
      val pgPort = sys.env.getOrElse("PG_PORT", "5432")
      val pgDb = sys.env.getOrElse("PG_DATABASE", "bank_db")
      val pgUser = sys.env.getOrElse("PG_USER", "postgres")
      val pgPassword = sys.env.getOrElse("PG_PASSWORD", "")
      val pgSchema = "public"
      val tableName = "cust_risk_triggers"

      val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"
      val connectionProperties = new java.util.Properties()
      connectionProperties.setProperty("user", pgUser)
      connectionProperties.setProperty("password", pgPassword)
      connectionProperties.setProperty("driver", "org.postgresql.Driver")

      logger.info(s"Loading to PostgreSQL: $jdbcUrl/$pgSchema.$tableName")
      logger.info(s"Records to load: ${df.count()}")

      df.write
        .mode(SaveMode.Overwrite)
        .jdbc(jdbcUrl, s"$pgSchema.$tableName", connectionProperties)

      logger.info(s"Successfully loaded data to $tableName")
    } catch {
      case exc: Exception =>
        logger.error("PostgreSQL load failed", exc)
        throw exc
    }
  }

  /**
    * Write audit file
    */
  def writeAuditFile(df: DataFrame): Unit = {
    try {
      val auditPath = "/tmp/audit/cust_risk_triggers/"
      logger.info(s"Writing audit file to: $auditPath")

      df.select(
        col("customerLpid"),
        col("riskScore"),
        col("riskIndicator"),
        col("processedAt")
      )
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(auditPath)

      logger.info(s"Audit file written successfully")
    } catch {
      case exc: Exception =>
        logger.error("Audit write failed", exc)
        // Don't fail the pipeline for audit file issues
    }
  }

  /**
    * Run the complete ETL pipeline
    */
  def run(): Unit = {
    try {
      // Build session
      buildSession()

      // Extract
      sourceDF = extract()

      // Transform
      val transformedDF = transform(sourceDF)

      // Load to PostgreSQL
      loadToPostgres(transformedDF)

      // Write audit
      writeAuditFile(transformedDF)

      logger.info("Pipeline execution completed successfully")
    } catch {
      case exc: Exception =>
        logger.error("Pipeline execution failed", exc)
        throw exc
    }
  }

  /**
    * Stop the Spark session
    */
  def stop(): Unit = {
    if (spark != null) {
      spark.stop()
      logger.info("SparkSession stopped")
    }
  }
}

