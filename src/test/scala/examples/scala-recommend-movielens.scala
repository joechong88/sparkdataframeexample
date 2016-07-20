/*  Scala codes to test recommendation on MovieLens database using ALS algorithm
    on MLib
*/

// this is used to implicitly convert an RDD to a DataFrame
import sqlContext.implicits._

// import Spark SQL types
import org.apache.spark.sql._

// import mllib recommendation data types
import org.apache.spark.mllib.recommendation.{ALS,
  MatrixFactorizationModel, Rating}

// define file location (either one)
//val folderLocation = "~/Envs/sparkdatafromexample/data/movielensmedium/"
val folderLocation = "hdfs://192.168.0.94:8020/user/jchong1/movielens"

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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
val results = sqlContext.sql(
  "SELECT movies.title, movierates.maxr, movierates.minr, movierates.cntu
  FROM (SELECT ratings.product, max(ratings.rating) as maxr,
  min(ratings.rating) as minr, count(distinct user) as cntu
  FROM ratings GROUP BY ratings.product) movierates
  JOIN movies on movierates.product = movies.MovieId
  ORDER BY movierates.cntu DESC")

// display top 20 results
results.show()
