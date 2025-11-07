package org.cscie88c.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.apache.spark.sql.Dataset

case class TripData(
    VendorID: Option[Int],
    tpep_pickup_datetime: Option[String],
    tpep_dropoff_datetime: Option[String],
    passenger_count: Option[Double],
    trip_distance: Option[Double],
    RatecodeID: Option[Double],
    store_and_fwd_flag: Option[String],
    PULocationID: Option[Int],
    DOLocationID: Option[Int],
    payment_type: Option[Double],
    fare_amount: Option[Double],
    extra: Option[Double],
    mta_tax: Option[Double],
    tip_amount: Option[Double],
    tolls_amount: Option[Double],
    improvement_surcharge: Option[Double],
    total_amount: Option[Double],
    congestion_surcharge: Option[Double],
    Airport_fee: Option[Double],
    cbd_congestion_fee: Option[Double]
)

case class DataIngestionSummary(
    row_count: Long,
    failures: Long
)

// Case classes for City Planners' analytics
case class PeakHourTripPercentage(borough: String, peakHourPercentage: Double)
case class WeeklyTripVolumeByBorough(
    borough: String,
    week: Int,
    tripVolume: Long
)
case class AvgTripTimeVsDistance(
    PULocationID: Int,
    DOLocationID: Int,
    avgTripTimeMinutes: Double,
    avgTripDistance: Double
)

// Case classes for Fleet Operators' analytics
case class WeeklyTotalTripsAndRevenue(
    week: Int,
    totalTrips: Long,
    totalRevenue: Double
)
case class AvgRevenuePerMileByBorough(
    borough: String,
    avgRevenuePerMile: Double
)
case class NightTripPercentageByBorough(
    borough: String,
    nightTripPercentage: Double
)

// Taxi Data Analyze Spark job
// Usage: spark-submit --class <main-class> <jar-file> <trip-data-path> <zone-lookup-path> <output-path> [start_week] [end_week]
// e.g., /opt/spark/bin/spark-submit --class org.cscie88c.spark.TaxiDataAnalyzeMainJob /path/to/jar /path/to/trips.parquet /path/to/taxi_zone_lookup.csv /path/to/output 1 4
// OR
// Create a batch job on GCP Dataproc with similar parameters. See https://cloud.google.com/dataproc-serverless/docs/overview

object TaxiDataAnalyzeMainJob {

  def main(args: Array[String]): Unit = {
    val (tripDataPath, zoneLookupPath, outpath, startWeek, endWeek) =
      args.toList match {
        case path1 :: path2 :: path3 :: Nil =>
          (path1, path2, path3, None, None)
        case path1 :: path2 :: path3 :: start :: Nil =>
          (path1, path2, path3, Some(start.toInt), None)
        case path1 :: path2 :: path3 :: start :: end :: Nil =>
          (path1, path2, path3, Some(start.toInt), Some(end.toInt))
        case _ => throw new IllegalArgumentException(
          "Usage: <tripDataPath> <zoneLookupPath> <outpath> [start_week] [end_week]")
      }

    implicit val spark = SparkSession
      .builder()
      .appName("GCPSparkJob")
      .master("local[*]")
      .config(
        "spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
      )
      .config(
        "spark.executor.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
      )
      .getOrCreate()

    // 1. Load input file (bronze layer)
    val inputTripData: Dataset[TripData] = loadInputFile(tripDataPath);

    // 2. Cleanup data (silver layer)
    val cleanTripData: Dataset[TripData] = cleanData(inputTripData)

    // 3. Calculate and save DataIngestionSummary
    val initialCount = inputTripData.count()
    val cleanCount = cleanTripData.count()
    val failures = initialCount - cleanCount

    import spark.implicits._
    val summary =
      Seq(DataIngestionSummary(row_count = cleanCount, failures = failures))
        .toDS()

    saveDataIngestionSummary(summary, outpath)

    // 4. Prepare data for KPI calculation
    val zoneDF = DataTransforms.loadZoneData(zoneLookupPath)
    val preparedDF =
      DataTransforms.prepareDataForKPIs(cleanTripData.toDF(), zoneDF)

    // 5. Apply optional week filter
    val filteredDF = DataTransforms.filterByWeek(preparedDF, startWeek, endWeek)

    // The 'filteredDF' is now ready for KPI calculations.
    // Cache it for better performance if it's used multiple times.
    filteredDF.cache()

    // 6. Calculate and save KPIs
    // City Planner KPIs
    val peakHourKPI = KPICalculations.peakHourTripPercentage(filteredDF)
    saveKPI(peakHourKPI.toDF(), outpath, "peak_hour_trip_percentage")

    val weeklyVolumeRaw = KPICalculations.weeklyTripVolumeByBorough(filteredDF)
    val weeklyVolumeWithAnomalies =
      KPICalculations.detectAnomalies(weeklyVolumeRaw)
    saveKPI(
      weeklyVolumeWithAnomalies,
      outpath,
      "weekly_trip_volume_with_anomalies"
    )

    val avgTimeVsDistanceKPI = KPICalculations.avgTripTimeVsDistance(filteredDF)
    saveKPI(avgTimeVsDistanceKPI.toDF(), outpath, "avg_trip_time_vs_distance")

    // Fleet Operator KPIs
    val weeklyRevenueKPI =
      KPICalculations.weeklyTotalTripsAndRevenue(filteredDF)
    saveKPI(weeklyRevenueKPI.toDF(), outpath, "weekly_total_trips_and_revenue")

    val avgRevenuePerMileKPI =
      KPICalculations.avgRevenuePerMileByBorough(filteredDF)
    saveKPI(
      avgRevenuePerMileKPI.toDF(),
      outpath,
      "avg_revenue_per_mile_by_borough"
    )

    val nightTripPercentageKPI =
      KPICalculations.nightTripPercentageByBorough(filteredDF)
    saveKPI(
      nightTripPercentageKPI.toDF(),
      outpath,
      "night_trip_percentage_by_borough"
    )

    // Unpersist the cached DataFrame
    filteredDF.unpersist()

    spark.stop()
  }

  def loadInputFile(
      filePath: String
  )(implicit spark: SparkSession): Dataset[TripData] = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(filePath)
      .as[TripData]
  }

  def cleanData(
      inputData: org.apache.spark.sql.Dataset[TripData]
  ): org.apache.spark.sql.Dataset[TripData] = {
    // Data Quality Checks:
    // 1. Null checks for key columns
    // 2. Range checks for numerical values (e.g. trip_distance > 0)
    // 3. Referential integrity checks for location IDs
    inputData.filter(
      F.col("PULocationID").isNotNull &&
        F.col("DOLocationID").isNotNull &&
        F.col("trip_distance") > 0 &&
        F.col("total_amount") >= 0 &&
        F.col("PULocationID").between(1, 265) &&
        F.col("DOLocationID").between(1, 265) &&
        F.col("tpep_pickup_datetime").isNotNull &&
        F.col("tpep_dropoff_datetime").isNotNull &&
        F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime") &&
        F.col("passenger_count") > 0
    )
  }

  def saveDataIngestionSummary(
      summary: org.apache.spark.sql.Dataset[DataIngestionSummary],
      outputPath: String
  ): Unit = {
    summary
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/ingest_summary/csv")
    summary.write
      .mode("overwrite")
      .parquet(s"$outputPath/ingest_summary/parquet")
  }

  def saveKPI(
      kpiDF: DataFrame,
      basePath: String,
      kpiName: String
  ): Unit = {
    val outputPath = s"$basePath/$kpiName"
    kpiDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/csv")
    kpiDF.write.mode("overwrite").parquet(s"$outputPath/parquet")
  }

}
