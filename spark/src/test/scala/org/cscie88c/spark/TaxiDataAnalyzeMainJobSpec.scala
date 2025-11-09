package org.cscie88c.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.JavaConverters._

case class TestKPI(id: Int, value: String)

class TaxiDataAnalyzeMainJobSpec
    extends AnyFunSuite
    with Matchers
    with SparkTest {

  test("loadInputFile should load trip data from a Parquet file correctly") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    val tempDir = Files.createTempDirectory("test-parquet").toFile
    val tempFilePath = tempDir.getAbsolutePath

    try {
      // Sample data based on the TripData case class
      val sampleData = Seq(
        TripData(
          Some(1),
          Some("2025-01-01 10:00:00"),
          Some("2025-01-01 10:15:00"),
          Some(1.0),
          Some(2.5),
          Some(1.0),
          Some("N"),
          Some(10),
          Some(20),
          Some(1.0),
          Some(12.5),
          Some(0.5),
          Some(0.5),
          Some(2.0),
          Some(0.0),
          Some(0.3),
          Some(15.8),
          Some(2.5),
          Some(0.0),
          None
        )
      ).toDF()

      // Write the sample data to a Parquet file
      sampleData.write.mode("overwrite").parquet(tempFilePath)

      // Call the function to test
      val loadedData = TaxiDataAnalyzeMainJob.loadInputFile(tempFilePath)
      val results = loadedData.collect()

      // Assertions
      results.length should be(1)
      results(0).VendorID should be(Some(1))
      results(0).trip_distance should be(Some(2.5))
    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("cleanData should filter out invalid trip records") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    val baseTrip = TripData(
      VendorID = Some(1),
      tpep_pickup_datetime = Some("2025-01-01 10:00:00"),
      tpep_dropoff_datetime = Some("2025-01-01 10:15:00"),
      passenger_count = Some(1.0),
      trip_distance = Some(2.5),
      RatecodeID = Some(1.0),
      store_and_fwd_flag = Some("N"),
      PULocationID = Some(10),
      DOLocationID = Some(20),
      payment_type = Some(1.0),
      fare_amount = Some(12.5),
      extra = Some(0.5),
      mta_tax = Some(0.5),
      tip_amount = Some(2.0),
      tolls_amount = Some(0.0),
      improvement_surcharge = Some(0.3),
      total_amount = Some(15.8),
      congestion_surcharge = Some(2.5),
      Airport_fee = Some(0.0),
      cbd_congestion_fee = None
    )

    val testData = Seq(
      baseTrip, // Valid record
      baseTrip.copy(PULocationID = None), // Invalid: Null PULocationID
      baseTrip.copy(DOLocationID =
        Some(300)
      ), // Invalid: DOLocationID out of range
      baseTrip.copy(trip_distance = Some(0.0)), // Invalid: Zero trip distance
      baseTrip.copy(total_amount =
        Some(-5.0)
      ), // Invalid: Negative total amount
      baseTrip.copy(passenger_count = Some(0.0)), // Invalid: Zero passengers
      baseTrip.copy(tpep_pickup_datetime =
        Some("2025-01-01 10:30:00")
      ) // Invalid: Pickup after dropoff
    ).toDS()

    // Call the function to test
    val cleanedData = TaxiDataAnalyzeMainJob.cleanData(testData)
    val results = cleanedData.collect()

    // Assertions
    // Only the single valid record should remain
    results.length should be(1)

    // Verify that the remaining record is the valid one
    val remainingTrip = results(0)
    remainingTrip.PULocationID should be(Some(10))
    remainingTrip.trip_distance should be(Some(2.5))
  }

  test(
    "saveDataIngestionSummary should save summary data in CSV and Parquet formats"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // 1. Setup: Create a temporary directory and sample data
    val tempDir = Files.createTempDirectory("test-summary-output").toFile
    val tempOutputPath = tempDir.getAbsolutePath

    val summaryData = Seq(
      DataIngestionSummary(row_count = 1000L, failures = 50L)
    ).toDS()

    try {
      // 2. Execute the function
      TaxiDataAnalyzeMainJob.saveDataIngestionSummary(
        summaryData,
        tempOutputPath
      )

      // 3. Verify CSV output
      val csvPath = s"$tempOutputPath/ingest_summary/csv"
      val csvFile = new File(csvPath)
      csvFile.exists() should be(true)
      csvFile.isDirectory() should be(true)

      val loadedCsv = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)
        .as[DataIngestionSummary]
        .collect()

      loadedCsv should contain theSameElementsAs Seq(
        DataIngestionSummary(1000L, 50L)
      )

      // 4. Verify Parquet output
      val parquetPath = s"$tempOutputPath/ingest_summary/parquet"
      val loadedParquet =
        spark.read.parquet(parquetPath).as[DataIngestionSummary].collect()
      loadedParquet should contain theSameElementsAs Seq(
        DataIngestionSummary(1000L, 50L)
      )
    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("saveKPI should save a KPI DataFrame in CSV and Parquet formats") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // 1. Setup: Create a temporary directory and sample data
    val tempDir = Files.createTempDirectory("test-kpi-output").toFile
    val tempOutputPath = tempDir.getAbsolutePath
    val kpiName = "my_test_kpi"

    val kpiData = Seq(
      TestKPI(1, "A"),
      TestKPI(2, "B")
    ).toDF()

    try {
      // 2. Execute the function
      TaxiDataAnalyzeMainJob.saveKPI(kpiData, tempOutputPath, kpiName)

      // 3. Verify CSV output
      val csvPath = s"$tempOutputPath/$kpiName/csv"
      val csvFile = new File(csvPath)
      csvFile.exists() should be(true)
      csvFile.isDirectory() should be(true)

      val loadedCsv = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)
        .as[TestKPI]
        .collect()
      loadedCsv should contain theSameElementsAs Seq(
        TestKPI(1, "A"),
        TestKPI(2, "B")
      )

      // 4. Verify Parquet output
      val parquetPath = s"$tempOutputPath/$kpiName/parquet"
      val loadedParquet = spark.read.parquet(parquetPath).as[TestKPI].collect()
      loadedParquet should contain theSameElementsAs Seq(
        TestKPI(1, "A"),
        TestKPI(2, "B")
      )
    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("main function should throw an exception for incorrect arguments") {
    val thrown = intercept[IllegalArgumentException] {
      // Call main with an incorrect number of arguments
      TaxiDataAnalyzeMainJob.main(Array("arg1", "arg2"))
    }
    thrown.getMessage should include(
      "Usage: <tripDataPath> <zoneLookupPath> <outpath> [start_week] [end_week]"
    )
  }
}
