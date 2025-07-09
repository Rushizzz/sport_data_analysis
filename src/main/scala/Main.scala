import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import sun.plugin.javascript.navig.JSType

object Main {
  def writeOutput(DF: DataFrame, questionNo: String): Unit = {
    DF.write
      .mode("overwrite")
      .json(s"./output/question_output/${questionNo}_question")
  }

  def questionsByScala_Solved(DF1: DataFrame, DF2: DataFrame, spark: SparkSession): Unit = {
    //    1.	How many olympics games have been held?
    val olympicsGamesCount = DF1.count()
    println("1.How many olympics games have been held? \n "+olympicsGamesCount)
    //    2.	List down all Olympics games held so far.
    val olympicsGames = DF1.select("Games").distinct()
    olympicsGames.show()
    writeOutput(olympicsGames, "2")
    //    3.	Mention the total no of nations who participated in each olympics game?
    val totalNoNation = DF2.select("region").distinct().count()
    import spark.implicits._
    val countDf = Seq(("Total_No_Nations", totalNoNation)).toDF("Total_No_Nations_Participated", "Count")
    countDf.show()
    writeOutput(countDf, "3")
    //    4.	Which year saw the highest and lowest no of countries participating in olympics?
    val stringsToRemove = List("Summer", "Winter", "Fall", "Spring")
    val patternToRemove = stringsToRemove.mkString("|")
    val cleanedDF = DF1.withColumn("Year", regexp_replace(col("Year"), patternToRemove, "" ))

    val countriesDfByYear = cleanedDF
      .groupBy("Year")
      .agg(count("NOC").alias("No_Of_Nations"))
    countriesDfByYear.filter(col("Year").contains("Summer")).show()

    val mostCountries = countriesDfByYear.orderBy(desc("No_Of_Nations")).limit(1)
    val minCountries = countriesDfByYear.orderBy(asc("No_Of_Nations")).limit(1)

    val minMostCountries = mostCountries.union(minCountries)
    minMostCountries.show()

    countriesDfByYear.filter(col("Year").contains("CAN")).show()

    cleanedDF.filter(col("Year").contains("CAN")).show()

    val updatedDF = DF1.filter(col("Year") =!= "CAN")

    println("This is updatedDF")
    updatedDF.filter(col("Year") === "CAN" ).show()

    val countriesDfByYear2 = updatedDF.
      groupBy("Year").
      agg(count("NOC").alias("No_Of_Nations"))

    val mostCountries2 = countriesDfByYear2.orderBy(desc("No_Of_Nations")).limit(1)
    mostCountries2.show()
    val minCountries2 = countriesDfByYear2.orderBy(asc("No_Of_Nations")).limit(1)
    minCountries2.show()

    val minMostCountries2 = mostCountries2.union(minCountries2)
    minMostCountries2.show()
    writeOutput(minMostCountries2, "4")

    //    5.	Which nation has participated in all of the olympic games?
    DF1.show(5)
    val nationsDF = DF1.groupBy(col("NOC")).agg(countDistinct(col("Year")).alias("Count"))
    nationsDF.orderBy(desc("Count")).show(false)
    val yearCount = DF1.select("Year").distinct().count()
    println(yearCount)

    nationsDF.where(col("Count") >= yearCount).show()
    writeOutput(nationsDF, "5")

    //    6.	Identify the sport which was played in all summer olympics.
    DF1.show(10)

    val sportsPlayedSummer = DF1.groupBy(col("Sport")).agg(count(col("Season") === "Summer").alias("Summer count"))
    sportsPlayedSummer.show(false)

    writeOutput(sportsPlayedSummer, "6")
    //    7.	Which Sports were just played only once in the olympics?
    DF1.show(10)
    val sportsPlayed = DF1
      .groupBy("Sport")
      .agg(count(col("Year")).alias("Count"))
      .where(col("Count") === 1 )

    val sportsPlayedOnce = sportsPlayed.select("Sport")
    sportsPlayedOnce.show(false)
    writeOutput(sportsPlayedOnce, "7")

    //    8.	Fetch the total no of sports played in each olympic games.
    DF1.show(5)
    val totalSportsPlayed = DF1.groupBy(col("Games"))
      .agg(count("Sport").alias("Count"))
      .orderBy(desc("Count"))

    totalSportsPlayed.show()
    writeOutput(totalSportsPlayed, "8")

    //    9.	Fetch details of the oldest athletes to win a gold medal.
    DF1.show(5)
    val goldAthletes = DF1.filter(col("Age") =!= "NA" && col("Medal") === "Gold")

    val oldestAge = goldAthletes
      .select(max(col("Age")).alias("Max Age"))

    val oldestathele = goldAthletes.where(col("Age") === oldestAge.head().get(0))
    oldestathele.show()
    writeOutput(oldestathele, "9")
    //    10.	Find the Ratio of male and female athletes participated in all olympic games.
    DF1.show(5)
    val maleFemaleCount = DF1.filter(col("Sex") isin("F", "M"))
      .groupBy("Sex")
      .agg(count("ID"))
    val row = maleFemaleCount.head()

    // Step 2: Convert result to Map[Sex -> count]
    val counts = maleFemaleCount
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap

    // Step 3: Extract counts safely
    val maleCount = counts.getOrElse("M", 1L)   // Avoid division by zero
    val femaleCount = counts.getOrElse("F", 1L)
    val total = DF1.count()

    // Step 4: Build your ratio string
    val data_value = s"${ total / maleCount } : ${ total / femaleCount }"



    // Step 5: Create DataFrame
    val ratioDF = Seq(data_value).toDF("ratio")
    ratioDF.show()
    writeOutput(ratioDF, "10")

    //    11.	Fetch the top 5 athletes who have won the most gold medals.
    DF1.show(5)
    val allColumnsExceptLast = DF1.columns.dropRight(1).map(col)

    val windowSpec = Window.orderBy($"Count".desc)

    val athletesWithGold = DF1.filter(col("Medal") === "Gold")
      .groupBy("ID","Name")
      .agg(count(col("Medal")).alias("Count"))
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .orderBy($"Count".desc)
      .filter( col("dense_rank") <= 5 )

    athletesWithGold.show()
    writeOutput(athletesWithGold, "11")

    //    12.	Fetch the top 5 athletes who have won the most medals (gold/silver/bronze).
//    val windowSpec12 = Window.orderBy($"Count".desc)
    val athleteMedals = DF1.filter(col("Medal").isin("Gold", "Silver", "Bronze"))
      .groupBy(col("ID"), col("Name"))
      .agg(count("Medal").alias("Count"))
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .orderBy($"Count".desc)
      .filter( col("dense_rank") <= 5 )

    athleteMedals.show()
    val ids = athleteMedals.select("ID").distinct()

    //    val filtered = DF1.join(ids, Seq("ID"), "inner")
    //    filtered.show()
    writeOutput(athleteMedals, "12")

    //    13.	Fetch the top 5 most successful countries in olympics. Success is defined by no of medals won.
    DF1.show(5)
//    val windowSpec13 = Window.orderBy($"Count".desc)

    val mostMedalCountries = DF1.filter(col("Medal").isin("Gold", "Silver", "Bronze"))
      .groupBy(col("NOC"))
      .agg(count("Medal").alias("Count"))
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .orderBy($"Count".desc)
      .filter( col("dense_rank") <= 5 )

    mostMedalCountries.show()
    writeOutput(mostMedalCountries, "13")
  }

  def questionsSolving(DF1: DataFrame, DF2: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._

    //    14.	List down total gold, silver and bronze medals won by each country.

    //    15.	List down total gold, silver and bronze medals won by each country corresponding to each olympic games.
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


  def main(args: Array[String]): Unit = {
    import SparkConfig. {athleteEvents, nocRegions, spark}

    //display schema
    athleteEvents.printSchema()
    nocRegions.printSchema()

//    questionsSolving(athleteEvents, spark)
//    athleteEvents.show(5)
//    questionsByScala_Solved(athleteEvents, nocRegions, spark)
    questionsSolving(athleteEvents, nocRegions, spark)
  }

}