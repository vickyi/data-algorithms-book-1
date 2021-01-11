package shrechak.chap03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.desc

/**
 * SparkSession实质上是SQLContext和HiveContext的组合（未来可能还会加上StreamingContext），
 * 所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
 * SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
 */
object TopN {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val inputPath = "src/main/scala/shrechak/chap03/sample-data.csv"

    val N = 10;

    val sortList = functions.udf((input: Seq[Row]) => {
      input.map { row: Row => (row.getInt(0), row.getInt(0)) }.sortBy(_._1).map(_._2)
    })

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)
      .groupBy("name")
      .agg(sum("value").as("count"))
      .orderBy(desc("count"))
      .limit(N)
      .show
  }
}
