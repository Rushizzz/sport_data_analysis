import org.apache.spark.sql.SparkSession

object SparkConfig {
  val spark =SparkSession.builder().appName("MINI_PROJECT").master("local[*]").getOrCreate()
  val athleteEvents = spark.read
    .option("header","true")
    .csv("./datasets/athlete_events.csv")
  val nocRegions = spark.read
    .option("header", "true")
    .csv("./datasets/noc_regions.csv")
}

