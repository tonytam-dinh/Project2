import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Benjamin_MostConfirmedCases {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder
      .appName("Hello Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
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

    val temp_table = spark.sql("SELECT ObservationDate,ProvinceorState,CountryorRegion,FLOOR(max(Confirmed)) as MaxConfirmed FROM TotalConfirmed GROUP BY ProvinceorState,CountryorRegion,ObservationDate")
    temp_table.createTempView("MaxCasesByStateCountry")

    val temp_table2 = spark.sql("SELECT ObservationDate, CountryorRegion, sum(MaxConfirmed) as MaxConfirmed FROM MaxCasesByStateCountry GROUP BY ObservationDate,CountryorRegion Order By ObservationDate DESC")
    temp_table2.createOrReplaceTempView("MaxCasesByCountry")
    println("Total cases worldwide")
    spark.sql("SELECT sum(MaxConfirmed) as WorldwideCases FROM MaxCasesByCountry GROUP BY ObservationDate LIMIT 1").show
    println("The 20 countries with the most cases")
    spark.sql("SELECT CountryorRegion, first(MaxConfirmed) as Cases FROM MaxCasesByCountry GROUP BY CountryorRegion ORDER BY 2 DESC").show
    spark.close()
  }
}