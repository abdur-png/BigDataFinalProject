//mapping state to region

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import spark.implicits._

val spark = SparkSession.builder.appName("hw9").getOrCreate()

// Function to read CSV file
def readCSV(path: String): DataFrame = {
  spark.read
    .format("csv")
    .option("header", true) // Assumes the CSV has a header
    .option("inferSchema", true) // Spark will automatically infer data types
    .load(path)
}

val filePath = "hw8_output/part-00000-9a2c5cba-51b9-4dc5-83b5-093524197572-c000.csv"

val cleanedData = readCSV(filePath)

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

// UDF for mapping state to region
val mapStateToRegion = udf((state: String) => stateToRegion.getOrElse(state, "Unknown"))

// Add 'Region' column to DataFrame
val dfWithRegion = cleanedData.withColumn("Region", mapStateToRegion($"State of Residence"))

dfWithRegion.coalesce(1).write.option("header","true").mode("overwrite").csv("finalProj")

