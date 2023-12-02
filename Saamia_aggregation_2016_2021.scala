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

val filePath = "finalProj/part-00000-579107c4-66f2-45d8-8917-755f594a0092-c000.csv"

val cleanedData = readCSV(filePath)

// Filter data for the years 2018 to 2021
val filteredDF = cleanedData.filter(col("Transaction Year").between(2016, 2021))

// Filter out rows with "Unknown" region
val intermediateDF = filteredDF.filter(col("Region") =!= "Unknown")

// Count occurrences of each region for each year
val regionCounts = intermediateDF.groupBy("Region", "Transaction Year").agg(count("Region").alias("EV_adoption_count"))

val orderedCounts = regionCounts.orderBy("Region", "Transaction Year")

orderedCounts.coalesce(1).write.option("header", "true").mode("overwrite").csv("aggregation16-21")
