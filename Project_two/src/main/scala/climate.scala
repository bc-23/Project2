import org.apache.spark.sql.{SparkSession, functions}
import DataframeCreation._
import org.apache.spark

//change the name to
object climate {

  def main(args: Array[String]): Unit = {

    println("Hello")

    // create a spark session
    // for Windows
//    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ClimateChange")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")

 createDataframe(spark)
//val data = [['tom', 10], ['nick', 15], ['juli', 14]]
//    val df = pd.DataFrame(data, columns = ['Name', 'Age'])
//bc test
val data1 = Array(1,2,3,4,5)
    val rdd1 = data1.length
    print(rdd1)
//cannot convert to dataframe?!!
  }

}
