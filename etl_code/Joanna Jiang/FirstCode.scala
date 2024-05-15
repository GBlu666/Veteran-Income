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




// Filter the RDD to include only rows with "Unemployment rate" in the index 2 column
val filteredRDD = cleanedRDD.filter(line => line.split(",")(2) == "Unemployment rate")

// Create a binary column based on whether the value in index 6 is above 4%
val withBinaryColumnRDD = filteredRDD.map(line => {
  val parts = line.split(",")
  val percentageOfVeterans = parts(6).toDouble
  val binaryValue = if (percentageOfVeterans > 4.0) "1" else "0"
  line + "," + binaryValue
})

// Print the first few rows of the RDD with the binary column
withBinaryColumnRDD.take(9).foreach(println)

// Mean-Median-Mode 1
// Get the value of the second column of the first row
val groupValue = cleanedRDD.first().split(",")(1)

// Filter the RDD to include only rows with the same value in the second column
val filteredRDD = cleanedRDD.filter(line => line.split(",")(1) == groupValue)

// Take the first 9 rows of the filtered RDD
val first9Rows = filteredRDD.take(9)

// Extract the values from the index 5 column and convert to Double
val values = first9Rows.map(line => line.split(",")(5).toDouble)

// Calculate the mean
val mean = values.sum / values.length

// Calculate the median
val sortedValues = values.sorted
val median = if (sortedValues.length % 2 == 0) {
  (sortedValues(sortedValues.length / 2 - 1) + sortedValues(sortedValues.length / 2)) / 2.0
} else {
  sortedValues(sortedValues.length / 2)
}

// Calculate the mode
val mode = values.groupBy(identity).mapValues(_.length).maxBy(_._2)._1

// Print the results
println(s"Mean: $mean")
println(s"Median: $median")
println(s"Mode: $mode")


// Mean-Median-Mode 2
// Filter the RDD to include only rows with "18 to 34 years" in the index 2 column
val filteredRDD = cleanedRDD.filter(line => line.split(",")(2) == "18 to 34 years")

// Extract the values from the index 6 column and convert to Double
val values = filteredRDD.map(line => line.split(",")(6).toDouble).collect()

// Calculate the mean
val mean = values.sum / values.length

// Calculate the median
val sortedValues = values.sorted
val median = if (sortedValues.length % 2 == 0) {
  (sortedValues(sortedValues.length / 2 - 1) + sortedValues(sortedValues.length / 2)) / 2.0
} else {
  sortedValues(sortedValues.length / 2)
}

// Calculate the mode
val mode = values.groupBy(identity).mapValues(_.length).maxBy(_._2)._1

// Print the results
println(s"Mean: $mean")
println(s"Median: $median")
println(s"Mode: $mode")



// Standard Deviation
// Filter the RDD to include only rows with "18 to 34 years" in the index 2 column
val filteredRDD = cleanedRDD.filter(line => line.split(",")(2) == "18 to 34 years")

// Extract the values from the index 6 column and convert to Double
val values = filteredRDD.map(line => line.split(",")(6).toDouble).collect()

// Calculate the mean
val mean = values.sum / values.length

// Calculate the standard deviation
val variance = values.map(value => math.pow(value - mean, 2)).sum / values.length
val standardDeviation = math.sqrt(variance)

// Print the results
println(s"Mean: $mean")
println(s"Standard Deviation: $standardDeviation")

