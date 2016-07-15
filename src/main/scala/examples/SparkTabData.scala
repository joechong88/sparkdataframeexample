// Processing tabular data with Spark SQL
// From "Getting Started with Spark"

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame
import sqlContext.implicits._

// import Spark SQL data types and Row.
import org.apache.spark.sql._

// load the data into a new RDD
val ebayText = sc.textFile("hdfs://192.168.0.94:8020/user/jchong1/sparkdataframeexample/ebay.csv")

// return the first element in this RDD
ebayText.first()

// define the schema using a case class
case class Auction(auctionid: String, bid: Float, bidtime: Float, 
	bidder: String, bidderrate: Integer, openbid: Float, price: Float, 
	item: String, daystolive: Integer)
	