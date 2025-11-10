package org.cscie88c.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTest extends BeforeAndAfterAll {
  self: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }
}
