package shrechak.chap01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, asc, collect_list, struct, col}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val inputPath = "src/main/scala/shrechak/sample-data.csv"

    // Solution 1: in-memory sort

    val sortList = functions.udf((input: Seq[Row]) => {
      input.map{row: Row => (row.getInt(0), row.getInt(0))}.sortBy(_._1).map(_._2)
    })

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
      .groupBy("name")
      .agg(collect_list(struct("time","value")).as("key2_list"))
      .withColumn("sorted_value_list", sortList(col("key2_list")))
      .select("name", "sorted_value_list")
      .show


    // Solution 2: composite key sort

    val sortBy = Window.partitionBy("name").orderBy(asc("time"))

    spark.read
      .option("header", "true")
      .csv(inputPath)
      .repartition(col("name"))
      .sortWithinPartitions("time")
      .groupBy("name").agg(collect_list("value").as("sorted_value_list"))
      .show
  }
}
