# Project2

# Project Description
- Create a Spark Application that processes COVID data
- Produce one or more .jar files for your analysis. Multiple smaller jar files are preferred. 
- 10 queries per group - find a trend - implement log4j or logging  - use Zeppelin (graphics and visuals) for showing trends and data analysis 
- Implement Agile Scrum methodology for project work 

# Technologies Used
- Apache Spark
- Spark SQL
- YARN
- HDFS and/or S3
- SBT
- Scala 2.12.10
- Git + GitHub
- Zeppelin

# Features
- Read data from https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset
- Produce analysis with Spark/SparkSQL
- Display Zeppelin visualization

# Getting started
(Assumes you have both HDFS,Zeppelin,Spark Submit, and SBT Configured)
- git clone https://github.com/tonytam-dinh/Project2.git
- cd to  ~/ProjectTwo/target/scala-2.11
- spark-submit --class Tony_MonthlyNewCases --master spark://localdomain --deploy-mode cluster projecttwo_2.11-0.1.0-SNAPSHOT.jar
- Visit localhost to view results
- For Zeppelin import the cfr_bydate_2GTGJYFXS.zpln file to view visualization

# Contributors
- Benjamin Ruiz
- Brady Dyson
- Brian McKenzie
- Tiffany Garner
- TonyTam Dinh
