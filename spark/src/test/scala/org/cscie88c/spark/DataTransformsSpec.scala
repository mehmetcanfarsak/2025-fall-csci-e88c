package org.cscie88c.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class DataTransformsSpec extends AnyFunSuite with Matchers with SparkTest {

  test("loadZoneData should load taxi zone data correctly") {
    val tempFilePath = Paths.get("temp_zone_data.csv")
    val csvContent = List(
      "LocationID,Borough,Zone,service_zone",
      "1,EWR,Newark Airport,EWR",
      "2,Queens,Jamaica Bay,Boro Zone"
    )
    Files.write(tempFilePath, csvContent.asJava)

    implicit val sparkSession = spark
    val zoneDF = DataTransforms.loadZoneData(tempFilePath.toString)

    zoneDF.schema.fields.map(_.name) should contain theSameElementsAs Seq(
      "LocationID",
      "Borough",
      "Zone",
      "service_zone"
    )

    val results = zoneDF.collect()
    results.length should be(2)
    results(0) should be(Row(1, "EWR", "Newark Airport", "EWR"))
    results(1) should be(Row(2, "Queens", "Jamaica Bay", "Boro Zone"))

    Files.delete(tempFilePath)
  }

  test("filterByWeek should filter data correctly") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      (1, "A"),
      (2, "B"),
      (3, "C"),
      (4, "D"),
      (5, "E")
    ).toDF("week", "value")

    // Test case 1: Only startWeek is provided
    val filteredByStart = DataTransforms.filterByWeek(testData, Some(3), None)
    val startResults = filteredByStart.select("week").as[Int].collect()
    startResults should contain theSameElementsAs Seq(3, 4, 5)

    // Test case 2: Only endWeek is provided
    val filteredByEnd = DataTransforms.filterByWeek(testData, None, Some(3))
    val endResults = filteredByEnd.select("week").as[Int].collect()
    endResults should contain theSameElementsAs Seq(1, 2, 3)

    // Test case 3: Both startWeek and endWeek are provided
    val filteredByBoth = DataTransforms.filterByWeek(testData, Some(2), Some(4))
    val bothResults = filteredByBoth.select("week").as[Int].collect()
    bothResults should contain theSameElementsAs Seq(2, 3, 4)

    // Test case 4: Neither startWeek nor endWeek is provided
    val notFiltered = DataTransforms.filterByWeek(testData, None, None)
    notFiltered.count() should be(5)
  }

  test("prepareDataForKPIs should prepare data correctly") {
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Sample trip data
    val tripData = Seq(
      (
        1,
        "2025-01-01 00:00:00",
        "2025-01-01 00:15:00",
        1,
        1.0,
        1,
        1,
        1,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0
      )
    ).toDF(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "RatecodeID",
      "store_and_fwd_flag",
      "PULocationID",
      "DOLocationID",
      "payment_type",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
      "Airport_fee",
      "cbd_congestion_fee"
    )

    // Sample zone data
    val zoneData = Seq(
      (1, "Manhattan", "New York", "Yellow Zone"),
      (2, "Queens", "Jamaica", "Boro Zone")
    ).toDF("LocationID", "Borough", "Zone", "service_zone")

    // Prepare the data
    val preparedData = DataTransforms.prepareDataForKPIs(tripData, zoneData)

    // Check if the required columns are present
    val expectedColumns = Seq(
      "VendorID",
      "tpep_pickup_datetime",
      "tpep_dropoff_datetime",
      "passenger_count",
      "trip_distance",
      "RatecodeID",
      "store_and_fwd_flag",
      "PULocationID",
      "DOLocationID",
      "payment_type",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
      "Airport_fee",
      "cbd_congestion_fee",
      "pickup_timestamp",
      "dropoff_timestamp",
      "trip_duration_minutes",
      "week",
      "pickup_borough",
      "pickup_zone",
      "pickup_service_zone",
      "dropoff_borough",
      "dropoff_zone",
      "dropoff_service_zone"
    )
    preparedData.columns should contain theSameElementsAs expectedColumns

    // Check if the data is joined correctly
    val results = preparedData.collect()
    results.length should be(1)
    results(0).getString(24) should be("Manhattan") // pickup_borough
  }
}
