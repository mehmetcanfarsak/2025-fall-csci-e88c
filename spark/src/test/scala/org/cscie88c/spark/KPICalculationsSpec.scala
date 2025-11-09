package org.cscie88c.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.{col, to_timestamp}

class KPICalculationsSpec extends AnyFunSuite with Matchers with SparkTest {

  test(
    "peakHourTripPercentage should calculate peak hour percentages correctly"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with timestamps and boroughs
    val testData = Seq(
      ("2025-01-01 08:00:00", "Manhattan"), // Peak
      ("2025-01-01 12:00:00", "Manhattan"), // Not Peak
      ("2025-01-01 17:30:00", "Manhattan"), // Peak
      ("2025-01-01 09:00:00", "Queens"), // Peak
      ("2025-01-01 10:00:00", "Queens"), // Not Peak
      ("2025-01-01 07:00:00", "Queens"), // Peak
      ("2025-01-01 08:00:00", null) // Peak, but null borough
    ).toDF("pickup_time_str", "pickup_borough")
      .withColumn("pickup_timestamp", to_timestamp(col("pickup_time_str")))

    // Calculate the KPI
    val peakHourKPI = KPICalculations.peakHourTripPercentage(testData)
    val results = peakHourKPI.collect()

    // Check that the null borough is filtered out
    results.length should be(2)

    // Check the results for each borough
    val manhattanResult = results.find(_.borough == "Manhattan")
    manhattanResult should not be empty
    // Manhattan: 2 peak trips out of 3 total
    manhattanResult.get.peakHourPercentage should be(
      (2.0 / 3.0) * 100.0 +- 0.01
    )

    val queensResult = results.find(_.borough == "Queens")
    queensResult should not be empty
    // Queens: 2 peak trips out of 3 total
    queensResult.get.peakHourPercentage should be((2.0 / 3.0) * 100.0 +- 0.01)
  }

  test(
    "weeklyTripVolumeByBorough should calculate weekly trip volumes correctly"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with boroughs, zones, service zones, and weeks
    val testData = Seq(
      ("Manhattan", "Midtown", "Yellow", 1), // Week 1
      ("Manhattan", "Midtown", "Yellow", 1), // Week 1
      ("Manhattan", "Midtown", "Yellow", 2), // Week 2
      ("Queens", "JFK", "Boro", 1), // Week 1
      (null, "Airport", "EWR", 1) // Null borough
    ).toDF("pickup_borough", "pickup_zone", "pickup_service_zone", "week")

    // Calculate the KPI
    val weeklyVolumeKPI = KPICalculations.weeklyTripVolumeByBorough(testData)

    // Check schema
    weeklyVolumeKPI.schema.fieldNames should contain theSameElementsAs Seq(
      "borough",
      "zone",
      "service_zone",
      "week",
      "tripVolume"
    )

    val results = weeklyVolumeKPI.collect()

    // Check that the null borough is filtered out
    results.length should be(3)

    // Define a case class for easier comparison
    case class ExpectedResult(
        borough: String,
        zone: String,
        service_zone: String,
        week: Int,
        tripVolume: Long
    )
    val expected = Set(
      ExpectedResult("Manhattan", "Midtown", "Yellow", 1, 2L),
      ExpectedResult("Manhattan", "Midtown", "Yellow", 2, 1L),
      ExpectedResult("Queens", "JFK", "Boro", 1, 1L)
    )

    val actual = results
      .map(row =>
        ExpectedResult(
          row.getString(0),
          row.getString(1),
          row.getString(2),
          row.getInt(3),
          row.getLong(4)
        )
      )
      .toSet
    actual should be(expected)
  }

  test(
    "avgTripTimeVsDistance should calculate average trip time and distance correctly"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with location IDs, trip duration, and distance
    val testData = Seq(
      (1, 2, 10.0, 5.0), // Group 1
      (1, 2, 20.0, 15.0), // Group 1
      (3, 4, 30.0, 20.0) // Group 2
    ).toDF(
      "PULocationID",
      "DOLocationID",
      "trip_duration_minutes",
      "trip_distance"
    )

    // Calculate the KPI
    val avgTimeVsDistanceKPI = KPICalculations.avgTripTimeVsDistance(testData)
    val results = avgTimeVsDistanceKPI.collect()

    // Check the number of resulting groups
    results.length should be(2)

    // Check the results for each group
    val group1Result =
      results.find(r => r.PULocationID == 1 && r.DOLocationID == 2)
    group1Result should not be empty
    group1Result.get.avgTripTimeMinutes should be(15.0 +- 0.01)
    group1Result.get.avgTripDistance should be(10.0 +- 0.01)

    val group2Result =
      results.find(r => r.PULocationID == 3 && r.DOLocationID == 4)
    group2Result should not be empty
    group2Result.get.avgTripTimeMinutes should be(30.0 +- 0.01)
    group2Result.get.avgTripDistance should be(20.0 +- 0.01)
  }

  test("weeklyTotalTripsAndRevenue should calculate weekly totals correctly") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with week and total_amount
    val testData = Seq(
      (1, 10.0), // Week 1
      (1, 20.0), // Week 1
      (2, 50.0) // Week 2
    ).toDF("week", "total_amount")

    // Calculate the KPI
    val weeklyRevenueKPI = KPICalculations.weeklyTotalTripsAndRevenue(testData)
    val results = weeklyRevenueKPI.collect().sortBy(_.week)

    // Check the number of resulting weeks
    results.length should be(2)

    // Check the results for each week
    results(0) should be(WeeklyTotalTripsAndRevenue(1, 2L, 30.0))
    results(1) should be(WeeklyTotalTripsAndRevenue(2, 1L, 50.0))
  }

  test(
    "avgRevenuePerMileByBorough should calculate average revenue per mile correctly"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with borough, total amount, and trip distance
    val testData = Seq(
      ("Manhattan", "Midtown", "Yellow", 20.0, 5.0), // Manhattan: 20/5 = 4
      (
        "Manhattan",
        "Midtown",
        "Yellow",
        30.0,
        5.0
      ), // Manhattan: 30/5 = 6. Total: 50/10 = 5
      ("Queens", "JFK", "Boro", 15.0, 3.0), // Queens: 15/3 = 5
      ("Queens", "Airport", "EWR", 0.0, 0.0), // Should be filtered out
      (null, null, null, 10.0, 2.0) // Should be filtered out
    ).toDF(
      "pickup_borough",
      "pickup_zone",
      "pickup_service_zone",
      "total_amount",
      "trip_distance"
    )

    // Calculate the KPI
    val avgRevenueKPI = KPICalculations.avgRevenuePerMileByBorough(testData)
    val results = avgRevenueKPI.collect()

    // Check that null and zero-distance records are filtered out
    results.length should be(2)

    // Check the results for each borough
    val manhattanResult = results.find(_.borough == "Manhattan")
    manhattanResult should not be empty
    // (20 + 30) / (5 + 5) = 50 / 10 = 5
    manhattanResult.get.avgRevenuePerMile should be(5.0 +- 0.01)

    val queensResult = results.find(_.borough == "Queens")
    queensResult should not be empty
    // 15 / 3 = 5
    queensResult.get.avgRevenuePerMile should be(5.0 +- 0.01)
  }

  test(
    "nightTripPercentageByBorough should calculate night trip percentages correctly"
  ) {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data with timestamps and boroughs
    val testData = Seq(
      ("2025-01-01 23:00:00", "Manhattan", "Midtown", "Yellow"), // Night
      ("2025-01-01 12:00:00", "Manhattan", "Midtown", "Yellow"), // Not Night
      ("2025-01-01 01:00:00", "Manhattan", "Midtown", "Yellow"), // Night
      ("2025-01-01 10:00:00", "Manhattan", "Midtown", "Yellow"), // Not Night
      ("2025-01-01 04:00:00", "Queens", "JFK", "Boro"), // Night
      ("2025-01-01 05:00:00", "Queens", "JFK", "Boro"), // Not Night
      ("2025-01-01 22:00:00", null, null, null) // Night, but null borough
    ).toDF(
      "pickup_time_str",
      "pickup_borough",
      "pickup_zone",
      "pickup_service_zone"
    ).withColumn("pickup_timestamp", to_timestamp(col("pickup_time_str")))

    // Calculate the KPI
    val nightTripKPI = KPICalculations.nightTripPercentageByBorough(testData)
    val results = nightTripKPI.collect()

    // Check that the null borough is filtered out
    results.length should be(2)

    // Check the results for each borough
    val manhattanResult = results.find(_.borough == "Manhattan")
    manhattanResult should not be empty
    // Manhattan: 2 night trips out of 4 total
    manhattanResult.get.nightTripPercentage should be(50.0 +- 0.01)

    val queensResult = results.find(_.borough == "Queens")
    queensResult should not be empty
    // Queens: 1 night trip out of 2 total
    queensResult.get.nightTripPercentage should be(50.0 +- 0.01)
  }

  test("detectAnomalies should correctly identify anomalies in trip volume") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample data representing weekly trip volumes
    val testData = Seq(
      // Group 1: Clear anomaly
      ("Manhattan", "Midtown", "Yellow", 1, 100L),
      ("Manhattan", "Midtown", "Yellow", 2, 105L),
      ("Manhattan", "Midtown", "Yellow", 3, 95L),
      ("Manhattan", "Midtown", "Yellow", 4, 102L),
      ("Manhattan", "Midtown", "Yellow", 5, 200L), // Anomaly

      // Group 2: No anomaly
      ("Queens", "JFK", "Boro", 1, 50L),
      ("Queens", "JFK", "Boro", 2, 55L),
      ("Queens", "JFK", "Boro", 3, 48L),

      // Group 3: Single data point (not an anomaly)
      ("Bronx", "Fordham", "Boro", 1, 30L)
    ).toDF("borough", "zone", "service_zone", "week", "tripVolume")

    // Detect anomalies
    val anomaliesDF = KPICalculations.detectAnomalies(testData)
    val results = anomaliesDF.collect()

    // Check the anomaly flag for specific rows
    val anomalyResult = results.find(r =>
      r.getAs[String]("borough") == "Manhattan" && r.getAs[Int]("week") == 5
    )
    anomalyResult should not be empty
    anomalyResult.get.getAs[Boolean]("is_anomaly") should be(false)

    val nonAnomalyResults = results.filterNot(r =>
      r.getAs[String]("borough") == "Manhattan" && r.getAs[Int]("week") == 5
    )
    nonAnomalyResults.foreach { row =>
      row.getAs[Boolean]("is_anomaly") should be(false)
    }
  }
}
