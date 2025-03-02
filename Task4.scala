import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
  def main(args: Array[String]) {
    // return an error message if the number of arguments is less than 2
    if (args.length < 2) {
      println("Error, 2 arguments are required: <input file> <output directory>")
      System.exit(1)
    }

    // Determine the number of executor instances and cores to use based on the input file size
    val (numExecutorInstances, numExecutorCores, partitionDivider) = if (args(0).endsWith("0.txt") || args(0).endsWith("5.txt")) {
      (1, 1, 1) // Small files
    } else if (args(0).endsWith("1.txt")) {
      (2, 4, 1) // Medium files
    } else {
      (8, 4, 2) // Large file
    }

    val conf = new SparkConf().setAppName("Task 4")
      .set("spark.driver.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.memory", "4g") // this will be provided as a cmd line argument but we explicitly set it to ensure it is the expected value
      .set("spark.executor.instances", numExecutorInstances.toString)
      .set("spark.executor.cores", numExecutorCores.toString)
    val sc = new SparkContext(conf)

    try {
      val textFile = sc.textFile(args(0), (numExecutorInstances * numExecutorCores) / partitionDivider)

      // process each row to collect user ratings
      // load into a map with a key for each movie title
      // and the value being another map with the user index as the key and their
      // rating for that particular movie as the value
      val movieRatingsMap = textFile.map { row =>
        val curRow = row.split(",", -1)
        val movieTitle = curRow(0)

        // collect ratings for each movie, skipping blank and invalid ratings
        // and store in a map
        val ratings = (for {
          i <- 1 until curRow.length
          if curRow(i) != null && curRow(i).nonEmpty
        } yield {
          try {
            // assume that all ratings will be valid integers between 1 and 5 and blank otherwise
            // we don't add any explicit logic to ignore invalid ratings values as it is stated that
            // all ratings will always be valid integers between 1 and 5 in the assignment requirements
            // thus we add the rating as long as we are able to convert it to an integer and don't
            // check whether it is in the range 1-5
            val rating = curRow(i).toInt
            Some((i, rating))
          } catch {
            // if the rating cannot be converted to an integer, we can just ignore it
            case _: NumberFormatException =>
              println(s"Ignoring invalid rating: ${curRow(i)} in row: $row")
              None
          }
        }).flatten.toMap // remove None values and convert to a Map

        (movieTitle, ratings) // create a tuple of movie title and its ratings
      }.collectAsMap().toMap // collect as a Map which will be broadcast

      // broadcast the movie ratings map
      val broadcastRatingsMap: Broadcast[Map[String, Map[Int, Int]]] = sc.broadcast(movieRatingsMap)

      // collect the movie titles to a sequence
      val movieTitles = movieRatingsMap.keys.toSeq
      // generate combinations of movie titles and compute similarity over each of these combinations
      // using the broadcastRatingsMap
      val output = sc.parallelize(movieTitles.combinations(2).toSeq)
        .map { case Seq(movie1, movie2) =>
          val sortedMovies = Seq(movie1, movie2).sorted
          val ratings1Map = broadcastRatingsMap.value(sortedMovies(0))
          val ratings2Map = broadcastRatingsMap.value(sortedMovies(1))
          val commonUsers = ratings1Map.keys.filter(ratings2Map.contains)
          val similarity = commonUsers.count(user => ratings1Map(user) == ratings2Map(user))
          (s"${sortedMovies(0)},${sortedMovies(1)},$similarity")
        }

      // save the output to a text file
      output.saveAsTextFile(args(1))

      // Introduce a delay to keep the application running for monitoring purposes on Spark UI
      // println("Job completed. Keeping the SparkContext alive for monitoring. Press Ctrl+C to exit.")
      // Thread.sleep(60000 * 2) // Keep the SparkContext alive for 2 minutes
    } catch {
      case e: Exception =>
        println("Error: An unexpected exception occurred during Task 4")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}