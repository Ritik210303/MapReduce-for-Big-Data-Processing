# 🚖 MapReduce for Big Data Processing – NYC Taxi Data Analysis  

## 📝 Project Overview  
This project demonstrates **MapReduce-based data processing** on **NYC Taxi Trip data**. The goal is to extract key insights, including **total trip revenue, busiest hours, and fare trends**, using a distributed computing approach. The project highlights how **Hadoop MapReduce** efficiently processes large datasets.  

## 🔍 Objectives  
- Implement **MapReduce jobs** to process NYC Taxi data.  
- Identify **peak travel hours and busiest locations**.  
- Analyze **total revenue trends based on trip duration and time of day**.  
- Optimize MapReduce performance using **combiner functions**.  

## 🛠️ Technologies & Tools  
- **Big Data Framework:** Hadoop, MapReduce  
- **Programming:** Python
- **Storage:** HDFS (Hadoop Distributed File System)  
- **Data Querying:** SQL  

## 📂 Dataset  
- The dataset consists of NYC Taxi trip records, including:  
  - **Pickup & drop-off timestamps**  
  - **Trip distance & duration**  
  - **Fare, taxes, and total revenue**

## 📊 Key Analysis Steps  
1. **Data Preprocessing:** Converted raw CSV files into a structured format for MapReduce.  
2. **MapReduce Job 1 – Revenue Calculation:** Summed total revenue from fares and surcharges.  
3. **MapReduce Job 2 – Peak Demand Hours:** Counted trips per hour to identify busiest times.  
4. **Performance Tuning:** Used **combiner functions and optimized memory allocation**.  

## 📌 Key Findings  
- **Peak demand occurs during 7 AM–9 AM and 5 PM–8 PM.**  
- **Weekends see higher long-distance trips, whereas weekdays have shorter city trips.**  
- **Revenue distribution shows price surges during bad weather and holiday seasons.**  

## 🚀 How to Run the Project  
- The dataset for the projects can be downloaded form the following liks:
<br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-01.csv
 <br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-02.csv
 <br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-03.csv
 <br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-04.csv
 <br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-05.csv
 <br> https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-06.csv
- RDS.pdf file explains how to RDS instance and upload the data
- Injectiontask.pdf file contains the code to create the HBase table. The file also includes the Sqoop command to ingest data from RDS into the HBase table.
- The Python code (batch_ingest.py) used to ingest the batch data to the HBase table.
- The Python codes used for the MapReduce tasks. The files should be labelled as mrtask_a.py, mrtask_b.py, and so on. MapReducetasks.pdf is a seprate file contains answers to the query and the screenshots of the results of the MapReduce tasks 
