// Processing tabular data with Spark SQL
// From "Getting Started with Spark"

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame
import sqlContext.implicits._

// import Spark SQL data types and Row.
import org.apache.spark.sql._

// loda the data into a new RDD
