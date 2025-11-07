package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object SparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SampleSparkJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq("Alice", "Bob", "Carol").toDF("name")
    val greeted = df.map(row => Utils.greet(row.getString(0)))

    greeted.show(false)
    spark.stop()
  }
}
