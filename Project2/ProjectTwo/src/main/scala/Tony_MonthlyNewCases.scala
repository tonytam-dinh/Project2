import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import sys.process._

object Tony_MonthlyNewCases {

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
    println("Monthly New Cases")

    spark.sql( " SELECT DATE_FORMAT(ObservationDate,\"M-y\") AS Month, ProvinceOrState, CountryOrRegion, max(Confirmed) AS MaxCases " +
               " FROM completeData " +
               " GROUP BY Month,CountryOrRegion,ProvinceOrState ").createTempView("monthlyData")

    spark.sql(" SELECT Month, FLOOR(sum(MaxCases)) AS CumulativeMonthlyCases" +
              " FROM monthlyData" +
              " GROUP BY Month" +
              " ORDER BY CumulativeMonthlyCases ASC").createTempView("monthlyCumulative")

    spark.sql(" SELECT Month, CumulativeMonthlyCases, " +
              " LAG(CumulativeMonthlyCases,1) OVER(" +
              " ORDER BY CumulativeMonthlyCases ASC) AS PreviousMonthCases" +
              " FROM monthlyCumulative" +
              " ORDER BY CumulativeMonthlyCases ASC").createTempView("monthlyCumulative2")

    spark.sql(" SELECT Month, CumulativeMonthlyCases - IFNULL(PreviousMonthCases,0) AS NewCases" +
              " FROM monthlyCumulative2").show()

    spark.close()
  }
}
