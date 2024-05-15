// predict earning with cohort years, 
// occupation in military, and payGrade
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

val occuprRDD = sc.textFile("occu-pr.csv")
val Head = occuprRDD.first()
val NoHeader = occuprRDD.filter(line => !line.equals(Head))
NoHeader.collect().foreach(println)

val preparedRDD = NoHeader.map(line => {
  val columns = line.split(",")
  (columns(0).toInt, // CohortYears
   columns(1), // LabelDODOccCode
   columns(2), // PayGrade
   columns(3).toInt, // Year1Emp
   columns(4).toInt, // Year5Emp
   columns(5).toInt, // Year1NonEmp
   columns(6).toInt, // Year5NonEmp
   columns(7).toInt, // Year1P50Earnings
   columns(8).toInt) // Year5P50Earnings
})

// Convert the RDD to DataFrame using toDF and provide column names
val dataDF = preparedRDD.toDF("CohortYears", "LabelDODOccCode", "PayGrade",
                              "Year1Emp", "Year5Emp", "Year1NonEmp",
                              "Year5NonEmp", "Year1P50Earnings", "Year5P50Earnings")

// Show the DataFrame to verify the structure and data
dataDF.show()


// Indexing and encoding categorical columns (LabelDODOccCode, PayGrade)
val indexer1 = new StringIndexer()
  .setInputCol("LabelDODOccCode")
  .setOutputCol("LabelDODOccCodeIndexed")
  .setHandleInvalid("keep")

val indexer2 = new StringIndexer()
  .setInputCol("PayGrade")
  .setOutputCol("PayGradeIndexed")
  .setHandleInvalid("keep")

val encoder1 = new OneHotEncoder()
  .setInputCol("LabelDODOccCodeIndexed")
  .setOutputCol("LabelDODOccCodeVec")

val encoder2 = new OneHotEncoder()
  .setInputCol("PayGradeIndexed")
  .setOutputCol("PayGradeVec")

// Assembling feature vector
val assembler = new VectorAssembler()
  .setInputCols(Array("CohortYears", "LabelDODOccCodeVec", "PayGradeVec"))
  .setOutputCol("features")

// Define the Linear Regression model
val lr = new LinearRegression()
  .setLabelCol("Year1P50Earnings")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.001)

// Create a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(indexer1, indexer2, encoder1, encoder2, assembler, lr))

// Fit the model
val model = pipeline.fit(dataDF)

// Make a prediction for CohortYears=4, LabelDODOccCode="Craftsworkers", PayGrade="E1-E5"
val testDF = Seq((4, "Craftsworkers", "E1-E5")).toDF("CohortYears", "LabelDODOccCode", "PayGrade")

// Make predictions
val predictions = model.transform(testDF)

// Show the predictions
predictions.select("CohortYears", "LabelDODOccCode", "PayGrade", "prediction").show()


// predict whether will be employed with 
// cohort years, occupation in military, and payGrade
import org.apache.spark.sql.functions._

// Calculate employment rates for 1 year and 5 years
val dataWithRates = dataDF.withColumn("Year1EmpRate", $"Year1Emp" / ($"Year1Emp" + $"Year1NonEmp"))
                          .withColumn("Year5EmpRate", $"Year5Emp" / ($"Year5Emp" + $"Year5NonEmp"))

dataWithRates.show()

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.Pipeline

// Convert rates to binary labels for logistic regression (1 if rate >= 0.5, otherwise 0)
val dataWithBinaryLabels = dataWithRates.withColumn("Year1EmpBinary", when($"Year1EmpRate" >= 0.5, 1).otherwise(0))

// Indexing and encoding categorical columns
val indexer1 = new StringIndexer()
  .setInputCol("LabelDODOccCode")
  .setOutputCol("LabelDODOccCodeIndexed")
  .setHandleInvalid("keep")

val indexer2 = new StringIndexer()
  .setInputCol("PayGrade")
  .setOutputCol("PayGradeIndexed")
  .setHandleInvalid("keep")

val encoder1 = new OneHotEncoder()
  .setInputCol("LabelDODOccCodeIndexed")
  .setOutputCol("LabelDODOccCodeVec")

val encoder2 = new OneHotEncoder()
  .setInputCol("PayGradeIndexed")
  .setOutputCol("PayGradeVec")

// Assembling feature vector
val assembler = new VectorAssembler()
  .setInputCols(Array("CohortYears", "LabelDODOccCodeVec", "PayGradeVec"))
  .setOutputCol("features")

// Define the Logistic Regression model
val lr = new LogisticRegression()
  .setLabelCol("Year1EmpBinary")
  .setFeaturesCol("features")
  .setMaxIter(10)

// Create a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(indexer1, indexer2, encoder1, encoder2, assembler, lr))

// Fit the model
val model = pipeline.fit(dataWithBinaryLabels)

// Example usage: Make a prediction for CohortYears=4, LabelDODOccCode="Craftsworkers", PayGrade="E1-E5"
val testDF = Seq((4, "Craftsworkers", "E1-E5")).toDF("CohortYears", "LabelDODOccCode", "PayGrade")
val predictions = model.transform(testDF)

// Show the predictions
predictions.select("CohortYears", "LabelDODOccCode", "PayGrade", "prediction").show()



// education level and earning
val eduprRDD = sc.textFile("edu-pr.csv")
val Head = eduprRDD.first()
val NoHeader = eduprRDD.filter(line => !line.equals(Head))
NoHeader.collect().foreach(println)

val dataRDD = NoHeader.map(line => {
  val columns = line.split(",").map(_.trim)  // Trim to avoid whitespace issues
  (columns(0).toInt,                        // Year as Int
   columns(1).toInt,                        // Cohort_years as Int
   columns(2),                              // Label of education as String
   columns(3).toInt,                        // Employed count as Int
   columns(4).toInt,                        // Unemployed count as Int
   columns(5).toInt)                        // Earnings as Int
})

val dataDF = dataRDD.toDF("year", "cohort_years", "label_education", "y1_emp", "y1_nonemp", "y1_p50_earnings")

// Calculate the employment rate and transform employment rate to binary for logistic regression
val dataWithRates = dataDF.withColumn("EmploymentRate", $"y1_emp" / ($"y1_emp" + $"y1_nonemp"))
                          .withColumn("EmploymentBinary", when($"EmploymentRate" >= 0.5, 1).otherwise(0))

// Indexing and encoding categorical column
val indexer = new StringIndexer()
  .setInputCol("label_education")
  .setOutputCol("label_education_Indexed")
  .setHandleInvalid("keep")

val encoder = new OneHotEncoder()
  .setInputCol("label_education_Indexed")
  .setOutputCol("label_education_Vec")

// Assembling feature vector including year and cohort_years
val assembler = new VectorAssembler()
  .setInputCols(Array("year", "cohort_years", "label_education_Vec"))
  .setOutputCol("features")

// Define the Linear Regression model for earnings
val lrEarnings = new LinearRegression()
  .setLabelCol("y1_p50_earnings")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.01)  // Set regularization parameter to 0.01

// Create a Pipeline for earnings prediction
val pipelineEarnings = new Pipeline()
  .setStages(Array(indexer, encoder, assembler, lrEarnings))

// Fit the earnings model
val modelEarnings = pipelineEarnings.fit(dataWithRates)

// Example usage: Predict for an example entry
val testDF = Seq((2020, 2, "High school diploma")).toDF("year", "cohort_years", "label_education")
val predictionsEarnings = modelEarnings.transform(testDF)

// Show the predictions
predictionsEarnings.select("year", "cohort_years", "label_education", "prediction").show()