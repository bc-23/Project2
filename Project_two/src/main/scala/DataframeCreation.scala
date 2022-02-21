import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object DataframeCreation {

  def createDataframe(spark: SparkSession): Unit = {

    val df1 = spark.read.format("csv").option("header", "true").load("input/anchor1.csv")
    df1.show(5)
    println(df1.count())
    println(df1.describe())

    val df2 = spark.read.format("csv").option("header", "true").load("input/nether1.csv")
    df2.show(5)
    println(df2.count())
    println(df2.describe())

    val df3 = spark.read.format("csv").option("header", "true").load("input/shang1.csv")
    df3.show(5)
    println(df3.count())
    println(df3.describe("AvgTemperature"))

    //    val df4 = spark.read.format("csv").option("header", "true").load("input/co2.csv")
    //    df4.show(5)
    //    println(df4.count())
    //
    //
    //    val df5 = spark.read.format("csv").option("header", "true").load("input/ghg.csv")
    //    df5.show(5)
    //    println(df5.count())

    val res1 = spark.sql("SELECT DATEDIFF('2017-08-25','2011-08-25') AS DateDiff;")
    df1.registerTempTable("anchor2")
    df2.registerTempTable("nether2")
    df3.registerTempTable("shang2")
    //    df1.createGlobalTempView("anchor2")
    //user will input 2 dates
    println("enter start date, end date in the format of YYYY-MM-DD")

    val startDate = readLine()
    val endDate = readLine()
    println("Pick a city from: a) anchorage b) amsterdam, or c)shanghai")
    val city = readLine()
    if (city == "a") {
      spark.sql(s"SELECT round(avg(AvgTemperature),2) AS avgTemp from anchor2 where Date between '$startDate' and '$endDate'").show()
    }
    else if (city == "b") {
      spark.sql(s"SELECT round(avg(AvgTemperature),2) AS avgTemp from nether2 where Date between '$startDate' and '$endDate'").show()
    }
    else if (city == "c") {
      spark.sql(s"SELECT round(avg(AvgTemperature),2) AS avgTemp from shang2 where Date between '$startDate' and '$endDate'").show()
    }
    else {
      print("not valid input")
    }

    // spark.sql("SELECT AvgTemperature AS avgTemperature from anchor where date between '2017-08-25' and '2011-08-25' ").show()
    //spark.sql("SELECT CAST((Year+Month+Day) AS DATE) from anchor as newdate;)")
    //spark.sql("SELECT City, Year, Rank() over (PARTITION BY month ORDER BY AvgTemperature DESC) FROM anchor2;").show(50)
  }
}
//import ghg and co2
    //start querying
    //optimize it : cache ????  TRY .cache
    //partition

//optimization- 6 techniques



