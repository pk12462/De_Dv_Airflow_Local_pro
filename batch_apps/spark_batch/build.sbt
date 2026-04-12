name := "bank-cust-risk-triggers-etl"
organization := "com.bank"
version := "1.0.0"

scalaVersion := "2.12.17"

// Spark Dependencies
val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Delta Lake
  "io.delta" %% "delta-core" % "3.2.0",

  // Database
  "org.postgresql" % "postgresql" % "42.7.1",

  // Config
  "com.typesafe" % "config" % "1.4.3",

  // Logging
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "ch.qos.logback" % "logback-classic" % "1.4.11",

  // HTTP Client
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3",

  // JSON
  "com.google.code.gson" % "gson" % "2.10.1",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.29" % Test
)

// Assembly settings for fat JAR
assembly / assemblyJarName := "bank-cust-risk-triggers-etl-1.0.0-assembly.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// Compiler settings
scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked"
)

// Test settings
Test / parallelExecution := false

