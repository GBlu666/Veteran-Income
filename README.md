### Project Structure Overview

This project includes several directories and files each serving specific purposes. Below is a breakdown of each component:

### Directories and Files

- **/ana_code**: Contains all analytic Scala code files for running the various prediction models.
- **/data_ingest**: Data dictionaries and explanations from the sources of data.
- **/etl_code**: Includes subdirectories for each team member with their respective ETL (Extract, Transform, Load) scripts.
- **/profiling_code**: Contains profiling scripts designed to evaluate and optimize the performance of the data processing and analytic tasks.
- **/test_code**: Is in the ana_code
- **/screenshots**: Screenshots demonstrating the execution of the analytics at various stages.
- **/readme**: This README.md file.

### How to Build the Code

**Environment Setup**: Ensure Scala and Apache Spark are properly installed and configured on your machine or server. This project was developed using Apache Spark, leveraging its RDD and DataFrame capabilities.
   

### How to Run the Code

To run the Scala scripts provided in the `/ana_code` directory, follow these steps:

1. **Start Spark Session**: Initiate a Spark session in your terminal or in an IDE that supports Scala (e.g., IntelliJ IDEA).

2. **Execute Commands**: Execute the commands in the scripts sequentially. Ensure that each step completes before moving on to the next to maintain the logical flow and dependencies between operations. (Just copy and paste each line from the .scala files are fine)

### Location of Input Data

- The input data for this project, `veteran-data.csv`, `edu-pr.csv`, and `occu-pr.csv` is utilized across various scripts for different predictive analytics. 
- Ensure that the dataset is placed in an accessible location and correctly referenced in the scripts.

### Results of a Run

- Results from each script are either displayed directly in the console or can be saved to an output file or database as configured in your environment.
- The scripts provide outputs such as model coefficients, intercepts, and predictions for future years as specified in the code.

### Access to Input Data

- The `veteran-data.csv`, `edu-pr.csv`, and `occu-pr.csv` used in this project can be found in the root of the HDFS directory used for previous assignments or in the `/ana_code` directory.

### Data Used
https://data.census.gov/table/ACSST1Y2022.S2101?q=Veterans
https://www.census.gov/data/experimental-data-products/veteran-employment-outcomes.html