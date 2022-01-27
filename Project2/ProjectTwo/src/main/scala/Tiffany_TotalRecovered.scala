import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import scala.sys.process._

object Tiffany_TotalRecovered {

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
    println("Total Recovered")
    spark.sql("SELECT MAX(Recovered) as TotalRecovered FROM TotalConfirmed GROUP BY ProvinceorState, CountryorRegion").createOrReplaceTempView("Recovered")
    spark.sql("SELECT FLOOR(SUM(TotalRecovered)) as TotalRecovered FROM Recovered").show()

    spark.close()
  }
}
