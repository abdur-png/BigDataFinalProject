# ElectroAnalytica: A Comprehensive Analysis of Electricity Consumption, Rates, and Electric Vehicles Purchase

<h2>Overview</h2>

"ElectroAnalytica" is a Scala-based big data analytics project, leveraging Apache Spark's powerful data processing capabilities. It focuses on analyzing large datasets related to electricity pricing, consumption, and electric vehicle (EV) adoption. The project's goal is to uncover insights and trends within these domains, providing valuable information for decision-making in energy management and policy formulation.

<h2>Repository Structure </h2>

<h3>1. Analytics Code (ana_code)</h3>
<ul>
  <li><strong>Electricity Pricing Analytics</strong>: This script performs an in-depth analysis of electricity rates. It includes functionalities like reading and preparing data, basic data exploration, yearly and regional analysis of electricity rates, statistical analysis of different rate types, and calculation of growth rates in electricity pricing. The script concludes by saving the analysis results and closing the Spark session.</li>
  <li><strong>Merged Analysis</strong>: This script is designed to analyze a merged dataset, focusing on demand, pricing, and EV adoption. It includes operations like reading data, schema printing, calculating top regions with the highest average demand and pricing, and analyzing EV adoption. Additionally, it performs an aggregate analysis over the years by region and calculates a correlation matrix for the aggregated data.</li>
  <li><strong>EV Adoption Analysis</strong>: This script focuses on analyzing factors influencing EV adoption, particularly examining the impact of odometer readings on EV purchase decisions. It includes reading CSV data, calculating statistics like mean, median, mode, and standard deviation for odometer readings, and analyzing the percentage of high odometer reading cars among used vehicles.</li>
  <li><strong>Electricity Consumption Analytics</strong>: This script analyzes electricity consumption patterns. It includes importing necessary libraries, reading data, calculating statistics for various consumption-related columns, aggregating data by region and time (year, month, hour), and analyzing growth rates in electricity demand and net generation.</li>
</ul>
<h3>2. Data Ingestion (data_ingest)</h3>
<p></strong></a>Merged Data Ingestion:</strong></a> This Scala script is responsible for ingesting and merging different datasets related to electricity consumption, pricing, and EV adoption. It defines paths to CSV files, reads the datasets, performs an inner join on 'region' and 'year' columns, and selects desired columns from the merged data.</p>
<h3>3. ETL Code (etl_code)</h3>
<ul>
  <li><strong>Electricity Pricing Cleaning</strong>: Dedicated to cleaning and preparing electricity pricing data. The script initializes a Spark session, defines a state-to-region mapping, reads multiple CSV files for different years, merges them, and performs data cleaning operations such as dropping null values and calculating average rates.</li>
  <li><strong>EV Adoption Cleaning</strong>: Focuses on cleaning the EV adoption dataset. It includes reading CSV data, selecting relevant columns, dropping rows with null values, and casting columns to appropriate data types for accurate analysis.</li>
  <li><strong>Electricity Consumption Cleaning</strong>: Cleans and consolidates electricity consumption data. The script includes loading and cleaning the data, defining functions to clean string columns, and applying these functions to specified columns for standardized data.</li>
</ul>

<h3>4. Profiling Code (profiling_code)</h3>
<ul>
  <li><strong>Electricity Pricing Profiling</strong>: Profiles the electricity pricing data by reading CSV files, merging them, and converting them into a DataFrame. It includes operations such as schema display, null value checks, and imputation of missing values using mean values.</li>
  <li><strong>EV Adoption Profiling</strong>: This script profiles the EV adoption dataset by displaying basic information about the DataFrame, counting null values in each column, and analyzing 'Odometer Reading' values to understand data quality.</li>
  <li><strong>Electricity Consumption Profiling</strong>: Dedicated to profiling the electricity consumption dataset. It includes data loading, schema display, and null value analysis to provide a comprehensive overview of the dataset's structure and quality.</li>
</ul>

<h3>5. Screenshots (screenshots)</h3>
<p>This section contains a collection of screenshots documenting the analytics process at various stages. Each screenshot demonstrates the execution and results of different steps in the data analysis pipeline, providing a visual representation of the project's functionality and output.</p>


<h2>How to Use</h2>
<p>Setup: Ensure Apache Spark and Scala are installed in your environment.<br>
Running Scripts: Navigate to the respective directories for analytics, ETL, or profiling. Run the Scala scripts within these directories to perform the desired operations.<br>
Customization: Modify the data paths in the scripts to point to your datasets.</p>

<h2>Dependencies</h2>
<ul>
  <li><strong>Apache Spark</strong>: A powerful open-source distributed computing system that provides fast data processing for big data.</li>
  <li><strong>Scala</strong>: A high-level programming language that is used to write the scripts in this project.</li>
</ul>

<h2>Datasets</h2>
  
  <h4>Electricity Consumption Data</h4>
  [Link to Dataset]
  Description of the dataset and its significance.
  
  <h4>Electricity Rates Data</h4>
  [Link to Dataset]
  Description of the dataset and its significance.
  
  <h4>Electric Vehicles Purchase Data</h4>
  [Link to Dataset]
  Description of the dataset and its significance.

<h2>Results</h2>

![list](/path/to/image.png)


