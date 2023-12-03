// Import necessary Spark SQL classes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType

// Define the function to read CSV
def readCSV(path: String): DataFrame = {
  // Create a SparkSession and read CSV file into a DataFrame
  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(path)

  // Return the DataFrame
  df
}

// Read the CSV file
val filePath = "hw8/EV_and_RegActivity.csv"
var EV_data = readCSV(filePath)

// Select relevant columns
EV_data = EV_data.select(
  "Model Year", "Make", "Model", "Electric Range", "Odometer Reading",
  "Odometer Code", "New or Used Vehicle", "Transaction Year", "State of Residence"
)

// Drop rows with null values in any column
EV_data = EV_data.na.drop()

// Additional cleaning steps
EV_data = EV_data.withColumn("Odometer Reading", col("Odometer Reading").cast(DoubleType))
EV_data = EV_data.withColumn("Transaction Year", col("Transaction Year").cast(DateType))

// State to region mapping
val stateToRegion = Map(
  "WA" -> "NW", "CA" -> "CAL", "UT" -> "SW", "VA" -> "SE", "OR" -> "NW", "TX" -> "TEX", "MI" -> "MIDW", 
  "TN" -> "TEN", "DE" -> "MIDA", "MN" -> "MIDW", "HI" -> "SW", "ID" -> "NW", "AL" -> "SE", "IN" -> "MIDW", 
  "WY" -> "NW", "GA" -> "SE", "FL" -> "FLA", "KY" -> "SE", "DC" -> "MIDA", "MD" -> "MIDA", "AZ" -> "SW", 
  "NE" -> "MIDW", "LA" -> "SE", "PA" -> "NE", "NC" -> "CAR", "NY" -> "NY", "SD" -> "MIDW", "AK" -> "NW", 
  "NM" -> "SW", "MO" -> "MIDW", "CO" -> "SW", "IL" -> "MIDW", "NV" -> "SW", "SC" -> "CAR", "MS" -> "SE", 
  "MT" -> "NW", "ND" -> "MIDW", "OH" -> "MIDW", "MA" -> "NE", "CT" -> "NE", "NJ" -> "NE", "WI" -> "MIDW", 
  "IA" -> "MIDW", "KS" -> "CENT", "OK" -> "SW", "AR" -> "SE", "NH" -> "NE", "ME" -> "NE"
)

// User-defined function (UDF) for mapping state to region
val mapStateToRegion = udf((state: String) => stateToRegion.getOrElse(state, "Unknown"))

// Add 'Region' column to DataFrame
val cleaned_data = EV_data.withColumn("Region", mapStateToRegion($"State of Residence"))

// Show the cleaned DataFrame
cleaned_data.show()

// Write the cleaned DataFrame to a CSV file
cleaned_data.coalesce(1).write.option("header", "true").mode("overwrite").csv("finalproject/Cleaned_Data")

