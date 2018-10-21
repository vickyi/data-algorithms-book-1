object SecondarySort extends App {
  val spark = SparkSession.builder()
    .master("local[2]")
    .getOrCreate()

  val inputPath = "src/main/scala/shrechak/sample-data.csv"

  // Solution 1: Get top N values

  val N = 10;

  val sortList = udf((input: Seq[Row]) => {
    input.map{row: Row => (row.getInt(0), row.getInt(0))}.sortBy(_._1).map(_._2)
  })

  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputPath)
    .groupBy("name")
    .agg(sum("value")).as("count"))
    .orderBy(desc("count"))
    .limit(N)
    .show
}
