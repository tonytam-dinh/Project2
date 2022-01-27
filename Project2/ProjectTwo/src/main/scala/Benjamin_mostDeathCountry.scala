import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Benjamin_mostDeathCountry {
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

    val temp_table = spark.sql("SELECT ProvinceorState,CountryorRegion,FLOOR(max(Deaths)) as MaxDeaths FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion")
    temp_table.createTempView("MaxDeathsByStateCountry")

    val temp_table2 = spark.sql("SELECT CountryorRegion, sum(MaxDeaths) as MaxDeaths FROM MaxDeathsByStateCountry GROUP BY CountryorRegion")
    temp_table2.createOrReplaceTempView("MaxDeathsByCountry")
    println("Total deaths worldwide")
    spark.sql("SELECT sum(MaxDeaths) AS WorldwideDeaths FROM MaxDeathsByCountry").show
    println("The 5 countries with the most deaths")
    spark.sql("SELECT CountryorRegion, MaxDeaths FROM MaxDeathsByCountry ORDER BY MaxDeaths DESC LIMIT 5").show
    spark.close()
  }
}