import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// Initialize Spark Session
val spark = SparkSession.builder.appName("Data Cleaning - Electricity Rates").getOrCreate()
import spark.implicits._

// Define the state-to-region mapping
val stateToRegion = Map(
  "WA" -> "NW", "CA" -> "CAL", "UT" -> "SW", "VA" -> "SE", "OR" -> "NW", "TX" -> "TEX", "MI" -> "MIDW", 
  "TN" -> "TEN", "DE" -> "MIDA", "MN" -> "MIDW", "HI" -> "SW", "ID" -> "NW", "AL" -> "SE", "IN" -> "MIDW", 
  "WY" -> "NW", "GA" -> "SE", "FL" -> "FLA", "KY" -> "SE", "DC" -> "MIDA", "MD" -> "MIDA", "AZ" -> "SW", 
  "NE" -> "MIDW", "LA" -> "SE", "PA" -> "NE", "NC" -> "CAR", "NY" -> "NY", "SD" -> "MIDW", "AK" -> "NW", 
  "NM" -> "SW", "MO" -> "MIDW", "CO" -> "SW", "IL" -> "MIDW", "NV" -> "SW", "SC" -> "CAR", "MS" -> "SE", 
  "MT" -> "NW", "ND" -> "MIDW", "OH" -> "MIDW", "MA" -> "NE", "CT" -> "NE", "NJ" -> "NE", "WI" -> "MIDW", 
  "IA" -> "MIDW", "KS" -> "CENT", "OK" -> "SW", "AR" -> "SE", "NH" -> "NE", "ME" -> "NE"
)

// User-Defined Function to map state to region
val mapStateToRegion = udf((state: String) => stateToRegion.getOrElse(state, null))

// Function to read CSV, add year, and  skip the first row
def readCsvWithYearAndSkipFirstRow(path: String, year: Int, skipFirstRow: Boolean): DataFrame = {
  val rdd = spark.sparkContext.textFile(path).mapPartitionsWithIndex { (idx, iter) =>
    if (idx == 0 && skipFirstRow) iter.drop(1) else iter
  }
  val df = rdd.map(row => s"$row,$year").toDF("value")
  val splitCols = split($"value", ",")
  df.select(
    splitCols.getItem(0).as("zip"),
    splitCols.getItem(1).as("eiaid"),
    splitCols.getItem(2).as("utility_name"),
    splitCols.getItem(3).as("state"),
    splitCols.getItem(6).cast(DoubleType).as("comm_rate"),
    splitCols.getItem(7).cast(DoubleType).as("ind_rate"),
    splitCols.getItem(8).cast(DoubleType).as("res_rate"),
    splitCols.getItem(9).as("year")
  )
}

// Paths to the CSV files (update these paths as needed)
val basePath = "/user/ar7165_nyu_edu/HW8"
val path2016 = s"$basePath/electricity_rates_2016.csv"
val path2017 = s"$basePath/electricity_rates_2017.csv"
val path2018 = s"$basePath/electricity_rates_2018.csv"
val path2019 = s"$basePath/electricity_rates_2019.csv"
val path2020 = s"$basePath/electricity_rates_2020.csv"
val path2021 = s"$basePath/electricity_rates_2021.csv"

// Read CSV files and add the year column
val df2016 = readCsvWithYearAndSkipFirstRow(path2016, 2016, skipFirstRow = false)
val df2017 = readCsvWithYearAndSkipFirstRow(path2017, 2017, skipFirstRow = true)
val df2018 = readCsvWithYearAndSkipFirstRow(path2018, 2018, skipFirstRow = true)
val df2019 = readCsvWithYearAndSkipFirstRow(path2019, 2019, skipFirstRow = true)
val df2020 = readCsvWithYearAndSkipFirstRow(path2020, 2020, skipFirstRow = true)
val df2021 = readCsvWithYearAndSkipFirstRow(path2021, 2021, skipFirstRow = true)

// Merge DataFrames
val mergedDF = df2016.union(df2017).union(df2018).union(df2019).union(df2020).union(df2021)

// Data Cleaning Process
val cleanedDF = mergedDF
  .na.drop() // Remove rows with null values
  .distinct() // Remove duplicate rows

// Write the cleaned data to a CSV file
val outputPath = "/user/ar7165_nyu_edu/FinalProject/cleaned_electricity_rates"
cleanedDF.write.option("header", "true").mode("overwrite").csv(outputPath)

// Stop the Spark session
spark.stop()
