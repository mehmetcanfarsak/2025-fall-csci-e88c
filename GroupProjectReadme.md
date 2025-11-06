# NYC Taxi Data Analysis Project

## Project Overview

This project analyzes the NYC Yellow Taxi trip records to extract valuable insights for different stakeholders, such as city planners and fleet operators. The project uses Apache Spark to process the data and calculate a variety of Key Performance Indicators (KPIs).

## Data

The following datasets are used in this project:

*   **NYC Yellow Taxi Trip Records**: This dataset contains detailed information about each taxi trip, including pickup and dropoff times and locations, trip distance, and fare amount. The project is designed to work with the January 2025 data, which can be downloaded from the [NYC TLC Trip Record Data website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
*   **Taxi Zone Lookup Table**: This dataset provides a mapping between the Location IDs used in the trip records and the corresponding borough, zone, and service zone.

## Data Pipeline

The data pipeline consists of the following steps:

1.  **Data Ingestion**: The raw trip data (in Parquet format) and the taxi zone lookup data (in CSV format) are loaded into Spark DataFrames.
2.  **Data Cleaning**: The trip data is cleaned to ensure data quality. This includes removing records with null values in key columns, filtering out trips with invalid distances or fare amounts, and ensuring that the pickup and dropoff times are consistent.
3.  **Data Transformation**: The trip data is enriched with additional information. This includes:
    *   Calculating the trip duration in minutes.
    *   Adding the week of the year for each trip.
    *   Joining the trip data with the zone lookup data to add pickup and dropoff borough information.
4.  **KPI Calculation**: A variety of KPIs are calculated from the transformed data. These KPIs are designed to provide insights for different stakeholders.
5.  **Data Output**: The calculated KPIs are saved to the output directory in both CSV and Parquet formats.

## KPIs

The following KPIs are calculated by the project:

### For City Planners

*   **Peak Hour Trip Percentage**: The percentage of trips that occur during peak hours (7-9 AM and 5-7 PM) for each borough.
*   **Weekly Trip Volume by Borough**: The total number of trips per week for each borough, with anomaly detection.
*   **Average Trip Time vs. Distance**: The average trip time and distance between each pickup and dropoff location.

### For Fleet Operators

*   **Weekly Total Trips and Revenue**: The total number of trips and the total revenue generated per week.
*   **Average Revenue Per Mile by Borough**: The average revenue per mile for each borough.
*   **Night Trip Percentage by Borough**: The percentage of trips that occur during the night (10 PM - 4 AM) for each borough.

## How to Run

1.  **Download the Data**: Download the NYC Yellow Taxi Trip Records for January 2025 from the [NYC TLC Trip Record Data website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place the `yellow_tripdata_2025-01.parquet` file in the `data/` directory. The `taxi_zone_lookup.csv` file is already included in the `data/` directory.

2.  **Compile the Project**: Compile the Spark application using `sbt`:

    ```bash
    sbt spark/assembly
    ```

3.  **Move the JAR file**: Copy the assembled JAR file to the Docker container:

    ```bash
    cp spark/target/scala-2.13/SparkJob.jar docker/apps/spark
    ```

4.  **Run the Spark Job**: Go into the bash of the container and submit the Spark job using the `spark-submit` command:

    ```bash
    docker compose -f docker-compose-spark.yml  up -d
    ```

    ```bash
    docker exec -it spark-master /bin/bash
    ```

    ```bash
    /opt/spark/bin/spark-submit \
      --class org.cscie88c.spark.TaxiDataAnalyzeMainJob \
      --master spark://spark-master:7077 \
      /opt/spark-apps/SparkJob.jar \
      /opt/spark-data/yellow_tripdata_2025-01.parquet \
      /opt/spark-data/taxi_zone_lookup.csv \
      /opt/spark-data/out/ \
      1 4
    ```

    The last two arguments (`1` and `4`) are optional and specify the start and end week for the analysis.

## Project Structure

*   `spark/src/main/scala/org/cscie88c/spark/`: This directory contains the Scala source code for the Spark application.
    *   `TaxiDataAnalyzeMainJob.scala`: The main entry point for the Spark job.
    *   `DataTransforms.scala`: Functions for data transformation and preparation.
    *   `KPICalculations.scala`: Functions for calculating the KPIs.
*   `data/`: This directory contains the input data files.
*   `docker/`: This directory contains the Docker configuration for the Spark cluster.
*   `build.sbt`: The sbt build file for the project.