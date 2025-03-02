import org.apache.spark.{SparkConf, SparkContext}

object Task2 {
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

    val conf = new SparkConf().setAppName("Task 2")
      .set("spark.driver.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.instances", numExecutorInstances.toString)
      .set("spark.executor.cores", numExecutorCores.toString)
    val sc = new SparkContext(conf)

    try {
      val textFile = sc.textFile(args(0), numExecutorInstances * numExecutorCores)

      // create an accumulator to sum the counts of non-blank ratings
      val accumulator = sc.longAccumulator("ratingCounts")

      // process each line to count the number of non-blank ratings
      // then add this number to the accumulator
      textFile.foreach { row =>
        val columns = row.split(",", -1)
        // count the non-blank columns, excluding the first column (movie title)
        // by using the tail method
        // assume that all ratings will be valid integers between 1 and 5 and blank otherwise
        // we don't add any explicit logic to ignore invalid ratings values as it is stated that
        // all ratings will always be valid integers between 1 and 5 in the assignment requirements
        // thus we add 1 to the number of non-blank ratings observed as long as the rating is
        // not blank, but it could theoretically take any non-blank value and still be counted
        // it doesn't need to be an integer between 1 and 5 technically
        val count = columns.tail.count(column => column != null && column.nonEmpty)
        // add the count of non-blank ratings to the accumulator
        accumulator.add(count)
      }

      // convert the accumulator value into an RDD and save it as text file
      sc.parallelize(Seq(accumulator.value), 1).saveAsTextFile(args(1))

      // Introduce a delay to keep the application running for monitoring purposes on Spark UI
      // println("Job completed. Keeping the SparkContext alive for monitoring. Press Ctrl+C to exit.")
      // Thread.sleep(60000 * 2) // Keep the SparkContext alive for 2 minutes
    } catch {
      case e: Exception =>
        println("Error: An unexpected exception occurred during Task 2")
        e.printStackTrace() // Print the stack trace of the exception
    } finally {
      // Ensure the SparkContext is stopped when finished to free
      // up resources for other applications
      sc.stop()
    }
  }
}
