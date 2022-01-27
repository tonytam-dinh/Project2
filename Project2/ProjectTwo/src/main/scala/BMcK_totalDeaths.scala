import io.netty.util.ResourceLeakDetector.Level
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive.sparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object BMcK_totalDeaths {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    //println("created spark session")
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

//    spark.sql("SELECT FLOOR(sum(Confirmed)) as TotalConfirmed FROM TotalConfirmed")
//      .show()

//    spark.sql("SELECT CountryorRegion, " +
//      "MAX(Deaths) / MAX(Confirmed) AS country_mortality_rate " +
//      "FROM TotalConfirmed GROUP BY CountryorRegion ORDER BY 2 DESC").show()

    println("Total Confirmed Deaths")
    spark.sql("SELECT MAX(Deaths) as TotalConfirmedDeaths FROM TotalConfirmed GROUP BY CountryorRegion, ProvinceOrState").createTempView("TempTCD")
    spark.sql("SELECT SUM(TotalConfirmedDeaths) FROM TempTCD").show()

//    spark.sql("SELECT MAX(Recovered) as TotalConfirmedRecoveries FROM TotalConfirmed GROUP BY CountryorRegion").createTempView("TempTCR")
//    spark.sql("SELECT FLOOR(SUM(TotalConfirmedRecoveries)) FROM TempTCR").show()
    
    spark.close()
  }
}