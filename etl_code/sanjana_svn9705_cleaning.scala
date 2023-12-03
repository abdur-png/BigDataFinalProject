//importing all the neccessary files
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.expressions.Window

//function to clean data
def cleanAndConsolidate(filePath: String): DataFrame = {
  val spark = SparkSession.builder.appName("Cleaning").getOrCreate()

  // Loading and Cleaning the Data
  val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath)

  // Defining the function to clean a string column
  def cleanStringColumn(df: DataFrame, colName: String): DataFrame = {
    df.withColumn(colName, regexp_replace(col(colName), ",", ""))
  }

  // List of columns to clean
  val columnsToClean = List("Balancing Authority", "Data Date", "Hour Number", "Local Time at End of Hour", "Demand Forecast (MW)", "Demand (MW)", "Net Generation (MW)", "Region")

  // Apply the cleanStringColumn function to each specified column
  var cleanedData = columnsToClean.foldLeft(data)((accDF, colName) => cleanStringColumn(accDF, colName))

  //-----------------CODE CLEANING-----------------
  // METHOD1: date formatting: Spliting the "Local Time at End of Hour" field into Date and Time
  cleanedData = cleanedData.withColumn("Local Date", split(col("Local Time at End of Hour"), " ").getItem(0))
  cleanedData = cleanedData.withColumn("Local Time", split(col("Local Time at End of Hour"), " ").getItem(1))
  // METHOD2: Binary Column: Creating a binary column based on Demand Forecast and Demand
  cleanedData = cleanedData.withColumn("Forecast Higher Than Demand", when(col("Demand Forecast (MW)") > col("Demand (MW)"), 1).otherwise(0))
  // METHOD3: Adding a column for the year
  cleanedData = cleanedData.withColumn("Year", year(to_date(col("Data Date"), "MM/dd/yyyy")))
  // METHOD4: Adding a column for the month/year only
  cleanedData = cleanedData.withColumn("MonthYear", date_format(to_date(col("Data Date"), "MM/dd/yyyy"), "MM/yyyy"))
  //-------------------------------------------

  // Selecting Desired Columns excluding the original "Local Time at End of Hour"
  val selectedColumns = cleanedData.select("Balancing Authority", "Data Date", "Year", "MonthYear", "Hour Number", "Local Date", "Local Time", "Demand Forecast (MW)",
    "Demand (MW)", "Net Generation (MW)", "Region", "Forecast Higher Than Demand")
  selectedColumns
}

val data2016a = cleanAndConsolidate("final/raw_data7.csv")
val data2016b = cleanAndConsolidate("final/raw_data8.csv")
val data2017a = cleanAndConsolidate("final/raw_data9.csv")
val data2017b = cleanAndConsolidate("final/raw_data10.csv")
val data2018a = cleanAndConsolidate("final/raw_data11.csv")
val data2018b = cleanAndConsolidate("final/raw_data12.csv")
val data2019a = cleanAndConsolidate("final/raw_data1.csv")
val data2019b = cleanAndConsolidate("final/raw_data2.csv")
val data2020a = cleanAndConsolidate("final/raw_data3.csv")
val data2020b = cleanAndConsolidate("final/raw_data4.csv")
val data2021a = cleanAndConsolidate("final/raw_data5.csv")
val data2021b = cleanAndConsolidate("final/raw_data6.csv")


// Concatenate the DataFrames vertically
val finalResult = data2016a.union(data2016b).union(data2017a).union(data2017b).union(data2018a).union(data2018b).union(data2019a).union(data2019b).union(data2020a).union(data2020b).union(data2021a).union(data2021b)

// Add a header to the final result
val header = Seq("Balancing Authority", "Data Date", "Year", "MonthYear", "Hour Number", "Local Date", "Local Time", "Demand Forecast (MW)",
  "Demand (MW)", "Net Generation (MW)", "Region", "Forecast Higher Than Demand")
val finalResultWithHeader = finalResult.toDF(header: _*)

// Save the final result with a header as a single CSV file
finalResultWithHeader.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/cleanedData")

//saving all the values as type double 
var cleandf = finalResultWithHeader.withColumn("Demand Forecast (MW)", col("Demand Forecast (MW)").cast(DoubleType)).withColumn("Demand (MW)", col("Demand (MW)").cast(DoubleType)).withColumn("Net Generation (MW)", col("Net Generation (MW)").cast(DoubleType))
//checking the amount of null values in the respective columns
val emptyValuesCount = cleandf
  .select(
    count(when(col("Demand Forecast (MW)").isNull || isnan(col("Demand Forecast (MW)")), true)).alias("Empty_Demand_Forecast"),
    count(when(col("Demand (MW)").isNull || isnan(col("Demand (MW)")), true)).alias("Empty_Demand"),
    count(when(col("Net Generation (MW)").isNull || isnan(col("Net Generation (MW)")), true)).alias("Empty_Net_Generation")
  )
  .show()

//looking at the correlation between net generation and demand/demand forecast
val correlationMatrix = cleandf
  .select(
    corr("Demand Forecast (MW)", "Net Generation (MW)").alias("Corr_Demand_Forecast_Net_Generation"),
    corr("Demand (MW)", "Net Generation (MW)").alias("Corr_Demand_Net_Generation")
  )
  .show()

//--------------------TRYING TO IMPUTE DATA 1------------------------ 
//METHOD 1: Trying to build an linear regression model to impute the values
//choosing net generation as the feature and demand as the lable 
//since the number of null values of net gen is very minimal, we drop that for analysis
val completeData = cleandf.na.drop("all", Seq("Net Generation (MW)")) 

//train test split for linear regression model 
val trainingData = completeData.filter(col("Demand (MW)").isNotNull)
val testData = completeData.filter(col("Demand (MW)").isNull)

//building the model and setting the feature and label for the model
val assembler = new VectorAssembler().setInputCols(Array("Net Generation (MW)")).setOutputCol("features") //creating this as the model only takes in vectors
val model = new LinearRegression().setFeaturesCol("features").setLabelCol("Demand (MW)").fit(assembler.transform(trainingData))

// Evaluate the model on the training set itself for accuracy 
val predictions = model.transform(assembler.transform(trainingData))

// Evaluate the model on the training set for accuracy
val evaluator = new RegressionEvaluator().setLabelCol("Demand (MW)").setPredictionCol("prediction").setMetricName("rmse") 
val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on training data: $rmse")
//As the correlation actual showed to us that the correlation is very low so the accuracy of the model is also low
//would have to look into more sofisticated models so we just imputed the values by the mean

//METHOD 2: Mean value imputation
val columnsToImpute = Array("Demand Forecast (MW)", "Demand (MW)", "Net Generation (MW)")
val imputer = new Imputer().setInputCols(columnsToImpute).setOutputCols(columnsToImpute.map(col => s"${col}_imputed")).setStrategy("mean")
val imputedData = imputer.fit(cleandf).transform(cleandf)
imputedData.show()

imputedData.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/imputedClean")
