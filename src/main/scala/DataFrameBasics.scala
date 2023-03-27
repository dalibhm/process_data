import org.apache.spark.sql.SparkSession

object TestSpark1 extends App {
  val spark = SparkSession.builder()
    .appName("TestSpark1")
    .config("spark.master", "local")
    .getOrCreate()

  val filename = "/Users/dali/workspace/zacks/data/SHARADAR_SFP_2_fb4f5d2244276f3cfeca03f46b122d99.csv"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)

  val ally = df.filter("ticker = 'ASET'")
  ally.show()

  spark.stop()
}