// code examples test by Joe Chong (self tutorial) 2016/07/15

// Processing Tabular Data with Spark SQL
// Help to get started with using Apache Spark DataFrames with Scala
// A Spark DataFrame is a distributed collection of data organized into named
// columns that provides operations for filter, group, or compute aggregates
// DataFrames - can be constructed from structured data files, existing RDDs,
// or external databases

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame
import sqlContext.implicits._

// import Spark SQL data types and Row
import org.apache.spark.sql._

// load the data into a new RDD (assume you're at the directory where the data
// file is located)
val ebayText = sc.textFile("tabdata/ebay.csv")

// return the first element in this RDD
ebayText.first()

// define the schema using a case class
case class Auction(auctionid: String, bid: Float, bidtime: Float,
  bidder: String, bidderrate: Integer, openbid: Float, price: Float,
  item: String, daystolive: Integer)

// create an RDD of Auction objects
val ebay = ebayText.map(_.split(",")).map(p => Auction(p(0), p(1).toFloat,
  p(2).toFloat, p(3), p(4).toInt, p(5).toFloat, p(6).toFloat, p(7), p(8).toInt))

// return the first element in this RDD
ebay.first()

// return the number of elements in the RDD
ebay.count()

// change eBay RDD of Auction objects to a DataFrame
val auction = ebay.toDF()

// display the top 20 rows of the new DataFrame
auction.show()

// return the schema of this DataFrame
auction.printSchema()

// after a DataFrame has been instantiated, it can be queried
// how many auctions were held
auction.select("auctionid").distinct.count

// how many bids per item?
auction.groupBy("auctionid", "item").count.show

// what's the minimum number of bids per item?
// what's the average? what's the max?
auction.groupBy("item", "auctionid").count
  .agg(min("count"), avg("count"), max("count")).show

// get the auctions with closing price > 100
val highprice = auction.filter("price > 100")

// display dataframe in a tabular format
highprice.show()

// register the DataFrame as a temp table, and use SQL statements to run
// against it using the methods provided by sqlContext
auction.registerTempTable("auction")

// how many bids per auction?
var results = sqlContext.sql(
  "SELECT auctionid, item, count(bid) FROM auction GROUP BY auctionid, item"
)

// display dataframe in a tabular format
results.show()

val results = sqlContext.sql(
  "SELECT auctionid, MAX(price) FROM auction GROUP BY item, auctionid"
)
results.show()
