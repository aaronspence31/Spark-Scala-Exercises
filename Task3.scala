import org.apache.spark.{SparkConf, SparkContext}

object Task3 {
  def main(args: Array[String]) {
    // return an error message if the number of arguments is less than 2
    if (args.length < 2) {
      println("Error, 2 arguments are required: <input file> <output directory>")
      System.exit(1)
    }

    // Determine the number of executor instances and cores to use based on the input file size
    val (numExecutorInstances, numExecutorCores) = if (args(0).endsWith("3.txt") || args(0).endsWith("4.txt")) {
      (2, 4) // Large files
    } else if (args(0).endsWith("0.txt") || args(0).endsWith("5.txt")) {
      (1, 1) // Small files
    } else {
      (1, 4) // Medium files
    }

    val conf = new SparkConf().setAppName("Task 3")
      .set("spark.driver.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.instances", numExecutorInstances.toString)
      .set("spark.executor.cores", numExecutorCores.toString)
    val sc = new SparkContext(conf)

    try {
      val textFile = sc.textFile(args(0), numExecutorInstances * numExecutorCores)

      val userRatingsList = textFile.flatMap { row =>
        val curRow = row.split(",", -1)

        // loop through all rows, and collect the users who put a review for that movie (non empty entry)
        // also need to account for users who did not specify a review by adding a 0 count for them
        // yield will construct a list of (i,0) and (i, 1) tuples
        (for {
          i <- 1 until curRow.length
        } yield {
          // assume that all ratings will be valid integers between 1 and 5 and blank otherwise
          // we don't add any explicit logic to ignore invalid ratings values as it is stated that
          // all ratings will always be valid integers between 1 and 5 in the assignment requirements
          // thus we add 1 to the number of ratings observed for each user as long as the rating is
          // not blank, but it could theoretically take any non-blank value and still be counted
          if (curRow(i) != null && curRow(i).nonEmpty) {
            (i, 1)
          } else {
            (i, 0)
          }
        })
      }

      // reduce by key to sum the counts of ratings for each user
      // then map the output to a string format
      val output = userRatingsList.reduceByKey(_ + _).map { case (user, count) =>
        s"$user,$count"
      }

      // save the output as a text file
      output.saveAsTextFile(args(1))

      // Introduce a delay to keep the application running for monitoring purposes on Spark UI
      // println("Job completed. Keeping the SparkContext alive for monitoring. Press Ctrl+C to exit.")
      // Thread.sleep(60000 * 2) // Keep the SparkContext alive for 2 minutes
    } catch {
      case e: Exception =>
        println("Error: An unexpected exception occurred during Task 3")
        e.printStackTrace() // Print the stack trace of the exception
    } finally {
      // Ensure the SparkContext is stopped when finished to free
      // up resources for other applications
      sc.stop()
    }
  }
}