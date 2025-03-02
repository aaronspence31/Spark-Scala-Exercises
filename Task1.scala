import org.apache.spark.{SparkConf, SparkContext}

object Task1 {
  def main(args: Array[String]) {
    // return an error message if the number of arguments is less than 2
    if (args.length < 2) {
      println("Error, 2 arguments are required: <input file> <output directory>")
      System.exit(1)
    }

    // Determine the number of executor instances and cores to use based on the input file size
    val (numExecutorInstances, numExecutorCores) = if (args(0).endsWith("0.txt") || args(0).endsWith("1.txt") || args(0).endsWith("5.txt")) {
      (1, 1) // Small - Medium files
    } else {
      (8, 4) // Large files
    }

    val conf = new SparkConf().setAppName("Task 1")
      .set("spark.driver.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.instances", numExecutorInstances.toString)
      .set("spark.executor.cores", numExecutorCores.toString)
    val sc = new SparkContext(conf)

    try {
      val textFile = sc.textFile(args(0), (numExecutorInstances * numExecutorCores))

      // process each line of the input file to find the highest rating indices
      val output = textFile.map { row =>
        val curRow = row.split(",", -1)
        val movieTitle = curRow(0)

        // extract the indices of the highest rating in the row with a single pass through the columns
        // store the max rating value in maxRating and the indices of the highest rating(s) in maxRatingIndices
        var maxRating = Int.MinValue
        var maxRatingIndices = List[Int]()
        for (i <- 1 until curRow.length) {
          if (curRow(i) != null && curRow(i).nonEmpty) {
            try {
              // assume that all ratings will be valid integers between 1 and 5 and blank otherwise
              // we don't add any explicit logic to ignore invalid ratings values as it is stated that
              // all ratings will always be valid integers between 1 and 5 in the assignment requirements
              // thus we will consider the rating as a possible max as long as it is not blank and an integer
              // and we do not check that it is in the range 1-5
              val rating = curRow(i).toInt
              if (rating > maxRating) {
                maxRating = rating
                maxRatingIndices = List(i) // replace the old list with a new one containing the current index
              } else if (rating == maxRating) {
                // because we are traversing each row in order, this will produce
                // a list with the indices in ascending order
                maxRatingIndices = maxRatingIndices :+ i // append the index to the list
              }
            } catch {
              // if the rating is not a valid integer, print an error message, ignore it and continue
              case e: NumberFormatException => println(s"Ignoring invalid rating: ${curRow(i)} in row: $row")
            }
          }
        }

        // return the movie title and the indices of the highest rating(s) as a formatted string
        // to be included in the output file
        s"$movieTitle,${maxRatingIndices.mkString(",")}"
      }

      // save the output as a text file
      output.saveAsTextFile(args(1))

      // Introduce a delay to keep the application running for monitoring purposes on Spark UI
      // println("Job completed. Keeping the SparkContext alive for monitoring. Press Ctrl+C to exit.")
      // Thread.sleep(60000 * 2) // Keep the SparkContext alive for 2 minutes
    } catch {
      case e: Exception =>
        println("Error: An unexpected exception occurred during Task 1")
        e.printStackTrace() // Print the stack trace of the exception
    } finally {
      // ensure the SparkContext is stopped when finished to free
      // up resources for other applications
      sc.stop()
    }
  }
}
