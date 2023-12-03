# ElectroAnalytica: A Comprehensive Analysis of Electricity Consumption, Rates, and Electric Vehicles Purchase

<h2>Overview</h2>

"ElectroAnalytica" is a Scala-based big data analytics project, leveraging Apache Spark's powerful data processing capabilities. It focuses on analyzing large datasets related to electricity pricing, consumption, and electric vehicle (EV) adoption. The project's goal is to uncover insights and trends within these domains, providing valuable information for decision-making in energy management and policy formulation.

<h2>Repository Structure and Detailed Description</h2>

<h3>1. Analytics Code (ana_code)</h3>
<ul>
  <li><strong>Electricity Pricing Analytics</strong>: This script performs an in-depth analysis of electricity rates. It includes functionalities like reading and preparing data, basic data exploration, yearly and regional analysis of electricity rates, statistical analysis of different rate types, and calculation of growth rates in electricity pricing. The script concludes by saving the analysis results and closing the Spark session.</li>
  <li><strong>Merged Analysis</strong>: This script is designed to analyze a merged dataset, focusing on demand, pricing, and EV adoption. It includes operations like reading data, schema printing, calculating top regions with the highest average demand and pricing, and analyzing EV adoption. Additionally, it performs an aggregate analysis over the years by region and calculates a correlation matrix for the aggregated data.</li>
  <li><strong>EV Adoption Analysis</strong>: This script focuses on analyzing factors influencing EV adoption, particularly examining the impact of odometer readings on EV purchase decisions. It includes reading CSV data, calculating statistics like mean, median, mode, and standard deviation for odometer readings, and analyzing the percentage of high odometer reading cars among used vehicles.</li>
  <li><strong>Electricity Consumption Analytics</strong>: This script analyzes electricity consumption patterns. It includes importing necessary libraries, reading data, calculating statistics for various consumption-related columns, aggregating data by region and time (year, month, hour), and analyzing growth rates in electricity demand and net generation.</li>
</ul>
<h3>2. Data Ingestion (data_ingest)</h3>
<p>Merged Data Ingestion: This Scala script is responsible for ingesting and merging different datasets related to electricity consumption, pricing, and EV adoption. It defines paths to CSV files, reads the datasets, performs an inner join on 'region' and 'year' columns, and selects desired columns from the merged data.</p>

<h3>3. ETL Code (etl_code)</h3>
<p>Electricity Pricing Cleaning: This script is dedicated to cleaning and preparing electricity pricing data. It initializes a Spark session, defines a state-to-region mapping, provides functions to read CSV files and map states to regions, reads multiple CSV files for different years, merges them, and performs data cleaning operations like dropping null values, calculating average rates, and identifying above-average residential rates.</p>

<h3>4. Profiling Code (profiling_code)</h3>
<p>The profiling code directory contains scripts for profiling different datasets. These scripts are crucial for understanding the data's characteristics, such as distribution, outliers, and missing values, which are essential for effective data analysis.</p>

<h3>5. Screenshots (screenshots)</h3>
<p>This directory contains PDFs of screenshots showcasing the results of various analytics performed. These visual representations are helpful in understanding the outcomes of the analyses and can be used for presentations or reports.</p>

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

<h2>Cleaning and Profiling</h2>
We have meticulously cleaned and profiled each dataset to ensure data quality and accuracy. This process involved handling missing values, correcting data types, and addressing any inconsistencies in the datasets.

<h2>Individual Analyses</h2>

  <h4>Electricity Consumption Analysis</h4>
  Summary of key findings and insights obtained from analyzing the 'Electricity Consumption' dataset.
  
  <h4>Electricity Rates Analysis</h4>
  Summary of key findings and insights obtained from analyzing the 'Electricity Rates' dataset.
  
  <h4>Electric Vehicles Purchase Analysis</h4>
  Summary of key findings and insights obtained from analyzing the 'Electric Vehicles Purchase' dataset.

<h2>Data Merging</h2>
We have combined the three datasets by mapping the data into regions. The merging process involved aggregating values for each region from the years 2016 to 2021.

<h2>Merged Analysis</h2>
A comprehensive analysis has been conducted on the merged dataset to identify potential linear correlations between different variables. This integrated approach provides a holistic view of the relationships between electricity consumption, rates, and electric vehicle purchases across regions.

<h2>How to Use</h2>
Instructions on how to replicate the analysis, including any dependencies or software requirements.

<h2>Conclusion</h2>
A brief summary of the overall findings and potential implications of the analyses.

