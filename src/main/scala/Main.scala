import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object Main {

  def questionsByScala_Solved(DF: DataFrame, spark: SparkSession): Unit = {
//    1.	How many olympics games have been held?
      DF.agg(count("*"))
        .show()
//    2.	List down all Olympics games held so far.
      DF.groupBy("games")
        .agg( collect_set("games") )
        .show(false)
  }

  def questionsSolving(DF: DataFrame, spark: SparkSession): Unit = {
    //    1.	How many olympics games have been held?
    var question = DF.agg(count("*"))
    question.show()
    writeOutput(question, "1")

    //    2.	List down all Olympics games held so far.
    question = DF.groupBy("games")
      .agg( collect_set("games") )
    question.show()
    writeOutput(question, "2")

    //    3.	Mention the total no of nations who participated in each olympics game?
    question = SparkConfig.athleteEvents
      .select("Games", "NOC")
      .distinct()
      .groupBy("Games")
      .agg(countDistinct("NOC").alias("Nations"))
      .orderBy("Games")

    question.show(false)
    writeOutput(question, "3")

    //    4.	Which year saw the highest and lowest no of countries participating in olympics?
    val participationByYear = SparkConfig.athleteEvents
      .select("Year", "NOC")
      .distinct()
      .groupBy("Year")
      .agg(countDistinct("NOC").alias("CountryCount"))

    val maxParticipation = participationByYear.orderBy(desc("CountryCount")).limit(1)
    val minParticipation = participationByYear.orderBy("CountryCount").limit(1)

    maxParticipation.show(false)
    minParticipation.show(false)

    writeOutput(maxParticipation, "4")
    writeOutput(minParticipation, "4")

    //    5.	Which nation has participated in all of the olympic games?
    val gamesPerNOC = SparkConfig.athleteEvents
      .select("Games", "NOC")
      .distinct()

    val totalGamesCount = gamesPerNOC.select("Games").distinct().count()

    val nocGameCount = gamesPerNOC
      .groupBy("NOC")
      .agg(countDistinct("Games").alias("GamesCount"))
      .filter(col("GamesCount") === totalGamesCount)

    val allGamesNation = nocGameCount
      .join(SparkConfig.nocRegions, Seq("NOC"), "left")
      .select("region")

    allGamesNation.show(false)

    writeOutput(allGamesNation, "5")

    //    6.	Identify the sport which was played in all summer olympics.
    val summerGames = SparkConfig.athleteEvents
      .filter(col("Season") === "Summer")
      .select("Year", "Sport")
      .distinct()

    val totalSummerYears = summerGames.select("Year").distinct().count()

    val sportInAllSummers = summerGames
      .groupBy("Sport")
      .agg(countDistinct("Year").alias("YearCount"))
      .filter(col("YearCount") === totalSummerYears)

    sportInAllSummers.show(false)
    writeOutput(sportInAllSummers, "6")
    //    7.	Which Sports were just played only once in the olympics?
    //
    //    8.	Fetch the total no of sports played in each olympic games.
    //
    //    9.	Fetch details of the oldest athletes to win a gold medal.
    //
    //    10.	Find the Ratio of male and female athletes participated in all olympic games.
    //
    //    11.	Fetch the top 5 athletes who have won the most gold medals.
    //
    //    12.	Fetch the top 5 athletes who have won the most medals (gold/silver/bronze).
    //
    //    13.	Fetch the top 5 most successful countries in olympics. Success is defined by no of medals won.
    //
    //    14.	List down total gold, silver and broze medals won by each country.
    //
    //    15.	List down total gold, silver and broze medals won by each country corresponding to each olympic games.
    //
    //    16.	Identify which country won the most gold, most silver and most bronze medals in each olympic games.
    //
    //    17.	Identify which country won the most gold, most silver, most bronze medals and the most medals in each olympic games.
    //
    //    18.	Which countries have never won gold medal but have won silver/bronze medals?
    //
    //    19.	In which Sport/event, India has won highest medals.
    //
    //    20.	Break down all olympic games where india won medal for Hockey and how many medals in each olympic games.

  }
  def writeOutput(DF: DataFrame, questionNo: String): Unit = {
    DF.write
      .mode("overwrite")
      .json(s"./output/question_output/${questionNo}_question")
  }

  def questionsByMySQL(DF: DataFrame, spark: SparkSession): Unit = {
      DF.createOrReplaceTempView("athlete_event_dataset")
//    1.	How many olympics games have been held?
//      val all_games = spark.sql("SELECT COUNT(*) AS all_games FROM athlete_event_dataset")
//      all_games.show()
//      writeOutput(all_games, "1st")

//    2.	List down all Olympics games held so far.
//        val games_name = spark.sql("SELECT games FROM athlete_event_dataset GROUP BY games")
//        games_name.show()
//        writeOutput(games_name, "2nd")
}

  def main(args: Array[String]): Unit = {
    import SparkConfig. {athleteEvents, nocRegions, spark}

    //display schema
    athleteEvents.printSchema()
    nocRegions.printSchema()

    questionsSolving(athleteEvents, spark)

  }
}