import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import sys.process._

object Tiffany_DayHighestConfirmed {

  def main(args: Array[String]): Unit = {


    System.setProperty("  hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add(StructField("SNo", IntegerType, true))
      .add(StructField("ObservationDate", DateType, true))
      .add(StructField("ProvinceOrState", StringType, true))
      .add(StructField("CountryOrRegion", StringType, true))
      .add(StructField("LastUpdate", StringType, true))
      .add(StructField("Confirmed", IntegerType, true))
      .add(StructField("Deaths", IntegerType, true))
      .add(StructField("Recovered", IntegerType, true))

    val data = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("hdfs://localhost:9000/user/tonylm/covid_data_fixed.csv")

    spark.sql("DROP TABLE IF EXISTS completeData")
    data.createTempView("completeData")

    println("Days With the Highest Confirmed New Cases")
    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-d-y\") AS Day, ProvinceOrState, CountryOrRegion, max(Confirmed) AS MaxCases " +
      " FROM completeData " +
      " GROUP BY Day,CountryOrRegion,ProvinceOrState ").createTempView("dailyData")

    spark.sql(" SELECT Day, FLOOR(sum(MaxCases)) AS CumulativeDailyCases" +
      " FROM dailyData" +
      " GROUP BY Day" +
      " ORDER BY CumulativeDailyCases ASC").createTempView("DailyCumulative")

    spark.sql(" SELECT Day, CumulativeDailyCases, " +
      " LAG(CumulativeDailyCases,1) OVER(" +
      " ORDER BY CumulativeDailyCases ASC) AS PreviousDailyCases" +
      " FROM dailyCumulative" +
      " ORDER BY CumulativeDailyCases ASC").createTempView("dailyCumulative2")

    spark.sql(" SELECT Day, CumulativeDailyCases - IFNULL(PreviousDailyCases,0) AS NewCases" +
      " FROM dailyCumulative2" +
      " ORDER BY NewCases DESC" +
      " LIMIT 10").show()

    spark.close()
  }
}
