package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.cscie88c.core.Utils

object SparkClusterJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ClusterSparkJob")
      .master(
        "spark://spark-master:7077"
      ) // connect to local Spark master in Docker
      .config("spark.executor.memory", "1g")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    val data = Seq("Edward", "Sumitra", "ChatGPT").toDF("name")
    val result = data.map(row => Utils.greet(row.getString(0)))

    result.show(false)

    spark.stop()
  }
}
