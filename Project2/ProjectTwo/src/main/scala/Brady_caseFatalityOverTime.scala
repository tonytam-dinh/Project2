import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import sys.process._

object Brady_caseFatalityOverTime {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
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

    spark.sql("DROP TABLE IF EXISTS TotalConfirmed")
    data.createTempView("TotalConfirmed")

    println("Case Fatality Over Time")
    spark.sql("SELECT CountryorRegion, date_format(ObservationDate,'M-y') AS Month," +
      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
      "FROM TotalConfirmed GROUP BY 1, 2 ORDER BY 1, 2").show()

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.createTempView("MaxCasesByStateCountry")
    println("The country with most cases and how many cases")
    spark.sql("SELECT CountryorRegion, MaxConfirmed FROM MaxCasesByStateCountry ORDER BY MaxConfirmed DESC LIMIT 1").show

    spark.close()
  }
}