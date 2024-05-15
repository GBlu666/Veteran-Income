// data cleaning

val veteranDataRDD1 = sc.textFile("veteran-data.csv")
veteranDataRDD1.collect().foreach(println)
val Head = veteranDataRDD1.first()
val NoHeader = veteranDataRDD1.filter(line => !line.equals(Head))

val cleanedRDD = NoHeader.map(line => {
  // Split the line into parts, handling commas within quotes
  val parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)
  val cleanedParts = parts.zipWithIndex.map {
    case (value, index) =>
      if (index == 4 || index == 6 || index == 8) {
        // Remove quotes, commas, percentage signs, and "(X)"
        value.replace("\"", "").replace(",", "").replace("%", "").replace("(X)", "")
      } else {
        // Remove quotes, commas, and "(X)"
        value.replace("\"", "").replace(",", "").replace("(X)", "")
      }
  }
  cleanedParts.mkString(",")
})

// Print the first few rows of the cleaned data
cleanedRDD.take(5).foreach(println)



// unemployment rate prediction for year 2023

val unemploymentRateRowsRDD = cleanedRDD.filter(line => {
    val columns = line.split(",") // Split the line into columns
    columns.length > 2 && columns(2) == "Unemployment rate" // Check if index 2 column is "Unemployment rate"
})

val preparedRDD = unemploymentRateRowsRDD.map(line => {
  val columns = line.split(",")
  (columns(0).toInt, columns(6).toDouble) // Assuming column 6 is a numeric type
})

// Sort the DataFrame by Year in ascending order
val sortedDataDF = dataDF.orderBy("Year")

// Set up VectorAssembler to assemble feature columns into a single vector column
val assembler = new VectorAssembler()
  .setInputCols(Array("Year"))
  .setOutputCol("features")

// Transform the DataFrame to include the features column
val finalData = assembler.transform(sortedDataDF).select("features", "UnemploymentRate").withColumnRenamed("UnemploymentRate", "label")

// Create the Linear Regression object
val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.001)

// Fit the model on the data
val lrModel = lr.fit(finalData)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Prepare the new data for prediction for the year 2023
val newDataFor2023 = Seq((2023)).toDF("Year")

// Assemble features for the new data for 2023
val newFeaturesDataFor2023 = assembler.transform(newDataFor2023)

// Make predictions for 2023
val newPredictionsFor2023 = lrModel.transform(newFeaturesDataFor2023)

// Show the predictions for 2023
newPredictionsFor2023.select("Year", "prediction").show()




//poverty rate difference prediction for 2023 (~percent lower than US)

// Filter rows for specific data related to poverty level
val povertyRateRowsRDD = cleanedRDD.filter(line => {
    val columns = line.split(",")
    columns.length > 2 && columns(2) == "Income in the past 12 months below poverty level"
})

// Prepare the RDD by selecting specific columns and calculating the difference
val preparedRDD = povertyRateRowsRDD.map(line => {
  val columns = line.split(",")
  val year = columns(0).toInt
  val povertyUS = columns(4).toDouble
  val povertyVeteran = columns(6).toDouble
  val difference = povertyUS - povertyVeteran
  (year, difference)
})

// Convert RDD to DataFrame
val dataDF = preparedRDD.toDF("Year", "PovertyDifference")

// Sort the DataFrame by Year in ascending order
val sortedDataDF = dataDF.orderBy("Year")

// Set up VectorAssembler to assemble feature columns into a single vector column
val assembler = new VectorAssembler()
  .setInputCols(Array("Year"))
  .setOutputCol("features")

// Transform the DataFrame to include the features column
val finalData = assembler.transform(sortedDataDF).select("features", "PovertyDifference").withColumnRenamed("PovertyDifference", "label")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.001)

// Fit the model on the data
val lrModel = lr.fit(finalData)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Prepare the new data for prediction for the year 2023
val newDataFor2023 = Seq((2023)).toDF("Year")

// Assemble features for the new data for 2023
val newFeaturesDataFor2023 = assembler.transform(newDataFor2023)

// Make predictions for 2023
val newPredictionsFor2023 = lrModel.transform(newFeaturesDataFor2023)

// Show the predictions for 2023
newPredictionsFor2023.select("Year", "prediction").show()




// income difference prediction 2023 (~dollars higher than median of US)

// Assuming `cleanedRDD` is your initial RDD
val filteredRDD = cleanedRDD.filter(line => {
  val columns = line.split(",")
  columns.length > 5 &&
  columns(1) == "MEDIAN INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)" &&
  columns(2) == "Civilian population 18 years and over with income"
})

// Prepare the RDD by selecting specific columns and calculating the difference
val preparedRDD = filteredRDD.map(line => {
  val columns = line.split(",")
  val year = columns(0).toInt
  val medianIncomeUS = columns(3).toDouble
  val medianIncomeVeterans = columns(5).toDouble
  val incomeDifference = medianIncomeVeterans - medianIncomeUS
  (year, incomeDifference)
})

// Convert RDD to DataFrame
val dataDF = preparedRDD.toDF("Year", "IncomeDifference")

// Sort the DataFrame by Year in ascending order
val sortedDataDF = dataDF.orderBy("Year")

// Set up VectorAssembler to assemble feature columns into a single vector column
val assembler = new VectorAssembler()
  .setInputCols(Array("Year"))
  .setOutputCol("features")

// Transform the DataFrame to include the features column
val finalData = assembler.transform(sortedDataDF).select("features", "IncomeDifference").withColumnRenamed("IncomeDifference", "label")

// Create the Linear Regression object
val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.001)

// Fit the model on the data
val lrModel = lr.fit(finalData)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Prepare the new data for prediction for the year 2023
val newDataFor2023 = Seq((2023)).toDF("Year")

// Assemble features for the new data for 2023
val newFeaturesDataFor2023 = assembler.transform(newDataFor2023)

// Make predictions for 2023
val newPredictionsFor2023 = lrModel.transform(newFeaturesDataFor2023)

// Show the predictions for 2023
newPredictionsFor2023.select("Year", "prediction").show()



//education level prediction for 2023

val filteredRDD = cleanedRDD.filter(line => {
  val columns = line.split(",")
  columns.length > 6 &&
  columns(1) == "EDUCATIONAL ATTAINMENT" &&
  (columns(2) == "Less than high school graduate" ||
   columns(2) == "High school graduate (includes equivalency)" ||
   columns(2) == "Some college or associate's degree" ||
   columns(2) == "Bachelor's degree or higher")
})

// Prepare the RDD by selecting specific columns
val preparedRDD = filteredRDD.map(line => {
  val columns = line.split(",")
  val year = columns(0).toInt
  val educationLabel = columns(2)
  val percentageOfVeteran = columns(6).toDouble
  (year, educationLabel, percentageOfVeteran)
})

// Convert RDD to DataFrame
val dataDF = preparedRDD.toDF("Year", "EducationLabel", "PercentageOfVeteran")

// Sort the DataFrame by Year in ascending order
val sortedDataDF = dataDF.orderBy("Year")

// Perform linear regression for each education level and predict for 2023
val educationLevels = sortedDataDF.select("EducationLabel").distinct.collect.map(_.getString(0))

educationLevels.foreach { level =>
  val filteredData = sortedDataDF.filter($"EducationLabel" === level)
  
  // Set up VectorAssembler to assemble feature columns into a single vector column
  val assembler = new VectorAssembler()
    .setInputCols(Array("Year"))
    .setOutputCol("features")

  // Transform the DataFrame to include the features column
  val finalData = assembler.transform(filteredData).select("features", "PercentageOfVeteran").withColumnRenamed("PercentageOfVeteran", "label")

  // Create the Linear Regression object
  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  // Fit the model on the data
  val lrModel = lr.fit(finalData)

  // Prepare the new data for prediction for the year 2023
  val newDataFor2023 = Seq((2023)).toDF("Year")

  // Assemble features for the new data for 2023
  val newFeaturesDataFor2023 = assembler.transform(newDataFor2023)

  // Make predictions for 2023
  val newPredictionsFor2023 = lrModel.transform(newFeaturesDataFor2023)

  // Show the predictions for 2023 for the current education level
  println(s"Predictions for 2023 for education level '$level':")
  newPredictionsFor2023.select("prediction").show()
}