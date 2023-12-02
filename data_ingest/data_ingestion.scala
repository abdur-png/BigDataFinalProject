import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.rdd.RDD
import scala.math.BigDecimal.RoundingMode

// Initialize Spark Session
val spark = SparkSession.builder().appName("Merge Electricity Data").getOrCreate()
import spark.implicits._

// Define the paths to the CSV files
val consumptionDataPath = "/user/svn9705_nyu_edu/final/avg_year/part-00000-7a178ddb-3b39-4ecf-a2ac-a4d883dfc5c0-c000.csv"
val pricingDataPath = "/user/ar7165_nyu_edu/FinalProject/growth_rates_by_region/part-00000-e8a8125e-19eb-4795-b8f2-852b12e478f1-c000.csv"
val vehiclelDataPath = "/user/ss14758_nyu_edu/aggregation16-21/part-00000-eefee01d-2758-4013-b4eb-1caf24c88c63-c000.csv"

val consumptionDF = spark.read.option("header", "true").option("inferSchema", "true").csv(consumptionDataPath)
val pricingDF = spark.read.option("header", "true").option("inferSchema", "true").csv(pricingDataPath)
val vehicleDF = spark.read.option("header", "true").option("inferSchema", "true").csv(vehiclelDataPath)

// Merge the datasets on 'region' and 'year' columns
val mergedDF = consumptionDF.join(pricingDF,
  consumptionDF("Region") === pricingDF("region") &&
  consumptionDF("Year") === pricingDF("year"),
  "inner"
).join(vehicleDF,
  consumptionDF("Region") === vehicleDF("Region") &&
  consumptionDF("Year") === vehicleDF("Transaction Year"),
  "inner"
)

// Select the desired columns from both DataFrames
// You might need to adjust this list to include the columns you need
val finalDF = mergedDF.select(
  consumptionDF("Region"),
  consumptionDF("Year"),
  consumptionDF("avg(Demand (MW)_imputed)"),
  consumptionDF("avg(Net Generation (MW)_imputed)"),
  pricingDF("avg(avg_rate)"),
  vehicleDF("EV_adoption_count")
)
finalDF.show()

finalDF.write.format("csv").option("header", "true").save("/user/ar7165_nyu_edu/FinalProject/mergedElectricityDataSanAb")
finalDF.write.format("csv").option("header", "true").save("/user/ss14758_nyu_edu/final_mergedData")
finalDF.write.format("csv").option("header", "true").save("/user/svn9705_nyu_edu/final/mergedData")

spark.stop()
