/*  Scala codes to test recommendation on MovieLens database using ALS algorithm
    on MLib
*/

// SQLContext entry point for working with structured data
// ** this is important to define first before the import statements below
// else the import statements would fail **
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame
import sqlContext.implicits._

// import Spark SQL types
import org.apache.spark.sql._

// import mllib recommendation data types
import org.apache.spark.mllib.recommendation.{ALS,
  MatrixFactorizationModel, Rating}

// define file location (either one)
val folderLocation = "movielens"
//val folderLocation = "hdfs://192.168.0.94:8020/user/jchong1/movielens"

// use the scala classes to define the Movie and User schemas
// input format MovieID::Title::Genre (in this case ignore Genre)
case class Movie(movieId: Int, title: String)

// input format UserID::Gender::Age::Occupation::Zip-code
case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

// define functions to parse the movies.dat, user.dat, rating.dat into corresponding
// Movie and User class
// function to parse input into Movie class
def parseMovie(str: String): Movie = {
  val fields = str.split("::")
  assert(fields.size == 3)
  Movie(fields(0).toInt, fields(1))
}

// function to parse input into User class
def parseUser(str: String): User = {
  val fields = str.split("::")
  assert(fields.size == 5)
  User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
}

// load the rating data into RDD
val ratingText = sc.textFile(folderLocation + "/" + "ratings.dat")

// return the first element of the RDD
ratingText.first()

// function to parse rating UserID::MovieID:Rating
// into org.apache.spark.mllib.recommendation.Rating class
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}

// create an RDD of Rating objects
val ratingsRDD = ratingText.map(parseRating).cache()

// count number of total ratings
val numRatings = ratingsRDD.count()

// count number of movies rated
val numMovies = ratingsRDD.map(_.product).distinct().count()

// count number of users who rated a movie
val numUsers = ratingsRDD.map(_.user).distinct().count()

// load the data into DataFrames
val usersDF = sc.textFile(folderLocation + "/" + "users.dat").map(parseUser).toDF()
val moviesDF = sc.textFile(folderLocation + "/" + "movies.dat").map(parseMovie).toDF()

// create a DataFrame from the ratingsRDD
val ratingsDF = ratingsRDD.toDF()

// register DataFrames as temp tables
ratingsDF.registerTempTable("ratings")
usersDF.registerTempTable("users")
moviesDF.registerTempTable("movies")

// print the schema of the temp tables
usersDF.printSchema()
moviesDF.printSchema()
ratingsDF.printSchema()

// get the min, max ratings along with the count of users who have rated a movie
// ** multi-line SQL statements in Scala code need to have triple double-quotes **
val results = sqlContext.sql(
  """SELECT movies.title, movierates.maxr, movierates.minr, movierates.cntu
  FROM (SELECT ratings.product, max(ratings.rating) as maxr,
  min(ratings.rating) as minr, count(distinct user) as cntu
  FROM ratings GROUP BY ratings.product) movierates
  JOIN movies on movierates.product = movies.movieId
  ORDER BY movierates.cntu DESC""")

// display top 20 results
results.show()

// show the top 10 most-active users and how many times they rated a movie
val mostActiveUsersSchemaRDD = sqlContext.sql(
  """SELECT ratings.user, count(*) as ct FROM ratings GROUP BY
  ratings.user ORDER BY ct DESC LIMIT 10""")

println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

// the above results will show user 4169 is the top rater, find out movies that
// this user had rated higher than 4
val results = sqlContext.sql(
  """SELECT ratings.user, ratings.product, ratings.rating, movies.title
  FROM ratings JOIN movies ON movies.movieId=ratings.product
  WHERE ratings.user=4169 and ratings.rating > 4""")

results.show()

// NOW WE'RE READY to run modelling on the ratings data
// 1. Use the MLLIB to learn the latent factors that can be used to predict
//    missing entries in the user-item association matrix
// 2. Start by separating the ratings data into training data (80%) and
// test data (20%)
// 3. Get recommendations for the training data, and evaluate the predictions
//    with test data
// 4. Do this iteratively with different subsets.

// Randomly split ratings RDD into training
// data RDD (80%) and test data RDD (20%)
val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

val trainingRatingsRDD = splits(0).cache()
val testRatingsRDD = splits(1).cache()

val numTraining = trainingRatingsRDD.count()
val numTest = testRatingsRDD.count()
println(s"Training: $numTraining, test: $numTest.")

// build a ALS user product matrix model with rank=20, iterations=10
val model = (new ALS().setRank(20).setIterations(10).run(trainingRatingsRDD))

// now we can use ModelFactorizationModel to make predictions
// first, get the movie predictions for the most active user, 4169

// get the top 4 movie predictions for user 4169
val topRecsForUser = model.recommendProducts(4169, 5)

// get movie titles to show with recommendations
val movieTitles = moviesDF.map(array => (array(0), array(1))).collectAsMap()

// print out top recommendations for user 4169 with titles
topRecsForUser.map(rating => (movieTitles(rating.product), rating.rating)).foreach(println)

// next we evaluate the model
// 1. get the user product pairs from testRatingsRDD to pass to the MatrixFactorizationModel predict method
// get user product pair from testRatingsRDD
val testUserProductRDD = testRatingsRDD.map{
  case Rating(user, product, rating) => (user, product)
}

// get predicted ratings to compare to test ratings
val predictionsForTestRDD = model.predict(testUserProductRDD)
predictionsForTestRDD.take(10).mkString("\n")

// now we compare test predictions with actual test ratings
// 1. put the predictions and test RDDs in this key, value pair format for joining ((user, product), rating)
// 2. print out the (user, product), (test rating, predicted rating) for comparison
// prepare predictions for comparison
val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}

// prepare test for comparison
val testKeyedByUserProductRDD = testRatingsRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}

// join the test with predictions
val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)

// print the (user, product), (test rating, predicted rating)
testAndPredictionsJoinedRDD.take(3).mkString("\n")

// find false positives
val falsePositives = (testAndPredictionsJoinedRDD.filter{case ((user, product),
  (ratingT, ratingP)) => (ratingT <=1 && ratingP >=4)})
falsePositives.take(2)
falsePositives.count()

// next we evaluate the model using Mean Absolute Error (MAE), i.e. the Absolute
// differences between the predicted and actual targets
val meanAbsoluteError = testAndPredictionsJoinedRDD.map{
  case((user, product), (testRating, predRating)) => val err = (testRating - predRating)
  Math.abs(err)
}.mean()
