import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, format_number}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import scala.io.StdIn.readLine

object Brady_caseFatalityOverTime_world {
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

    spark.sql("DROP TABLE IF EXISTS completeData")
    data.createTempView("completeData")

    //    Formats date.
    val maxForEachCountryMonth = spark.sql( " SELECT " +
      "DATE_FORMAT(ObservationDate,\"yyyy-MM\") AS Month, " +
      "CountryOrRegion, " +
      "ProvinceOrState, " +
      "MAX(Confirmed) AS maxCases, " +
      "MAX(Deaths) AS maxDeaths " +
      "FROM completeData " +
      "GROUP BY 1,2,3").toDF()

    val str3 = "maxForEachCountryMonth: For each month, and for each country within that month, " +
      "grabs the cumulative number of cases and deaths at the end of the month (the maximum for that month)."
    val maxForEachCountryMonth2 = maxForEachCountryMonth
      .withColumn("Month", date_format(col("Month"),"yyyy-MM"))
      .withColumn("CountryorRegion",col("CountryorRegion"))
      .withColumn("ProvinceOrState",col("ProvinceOrState"))
      .withColumn("maxCases",col("maxCases"))
      .withColumn("maxDeaths",col("maxDeaths"))
    maxForEachCountryMonth2.createTempView("maxForEachCountryMonth")
    println(str3)
    spark.sql("SELECT * FROM maxForEachCountryMonth").show()

    val str4 = "This outputs the cumulative total of cases and deaths worldwide for every month."
    spark.sql("SELECT Month, " +
      "FLOOR(SUM(maxCases)) AS totCumCases, " +
      "FLOOR(SUM(maxDeaths)) AS totCumDeaths " +
      "FROM maxForEachCountryMonth GROUP BY Month ORDER BY Month")
      .createOrReplaceTempView("cumulativeCasesAndDeathsByMonth")
    println(str4)
    spark.sql("SELECT * FROM cumulativeCasesAndDeathsByMonth").show()

    val str5 = "This outputs the cumulative cases and deaths for the current month AND the previous month (using LAG)."
    spark.sql("SELECT Month, totCumCases, IFNULL(LAG(totCumCases,1) OVER(ORDER BY totCumCases ASC),0) AS prevMonthCases, totCumDeaths, IFNULL(LAG(totCumDeaths,1) OVER(ORDER BY totCumDeaths ASC),0) AS prevMonthDeaths FROM cumulativeCasesAndDeathsByMonth").createOrReplaceTempView("monthComparisonsCasesDeaths")
    println(str5)
    spark.sql("SELECT * FROM monthComparisonsCasesDeaths").show()

    spark.sql("SELECT Month, totCumCases - prevMonthCases AS newCases, totCumDeaths - prevMonthDeaths AS newDeaths FROM monthComparisonsCasesDeaths").createOrReplaceTempView("newCasesDeaths")
    spark.sql("SELECT * FROM newCasesDeaths")

    val newCasesDeathsCFR = spark.sql("SELECT Month, newCases, newDeaths, newDeaths / newCases AS CFR FROM newCasesDeaths").toDF()
    val newCasesDeathsCFR2 = newCasesDeathsCFR
      .withColumn("newCases",col("NewCases"))
      .withColumn("newDeaths",col("newDeaths"))
      .withColumn("CFR",format_number(col("CFR"),2))
    //      createOrReplaceTempView("newCasesDeathsCFR")
    newCasesDeathsCFR2.show()

    spark.close()

  }
}