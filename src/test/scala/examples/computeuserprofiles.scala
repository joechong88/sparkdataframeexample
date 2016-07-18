// Compute user profiles with Spark
from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
import csv

conf = SparkConf().setAppName('ListenerSummarizer')
sc = SparkContext(conf=conf)

// read the CSV records with the individual track events, and make a PairRDD
// out of all rows. Use map() to convert each line of data into an array
// and then use reduceByKey() to consolidate all of the arrays

trackfile = sc.textFile('musicuserprofile/tracks.csv')

def make_tracks_kv(str):
  l = str.split(",")
  return [l[1], [[int(l[2], l[3], int(l[4]), l[5]]]]

  # make a k,v RDD out of the input data
  tbycust = trackfile.map(lambda line: make_tracks_kv(line)).reduceByKey(lambda a, b: a+b)

// pass a function to mapValues, resulting in a high-level profile can be computed
// from the components. The summary data above is available to compute basic
// statistics that can be used for display, using the colStats function from
// pyspark.mllib.stat

def compute_stats_byuser(tracks):
    mcount = morn = aft = eve = night = 0
    tracklist = []
    for t in tracks:
      trackid, dtime, mobile, zip = t
      if trackid not in tracklist:
        tracklist.append(trackid)
      d, t = dtime.split(" ")
      hourofday = int(t.split(":")[0])
      mcount += mobile
      if (hourofday < 5):
        night += 1
      elif (hourofday <12):
        morn += 1
      elif (hourofday < 17):
        aft += 1
      elif (hourofday < 22):
        eve += 1
      else:
        night += 1
      return [len(tracklist), morn, aft, eve, night, mcount]

# compute profile for each user
custdata = tbycust.mapValues(lamda a: compute_stats_byuser(a))

# compute aggregrate stats for entire track history
aggdata = Statistics.colStats(custdata.map(lambda x: x[1]))

// output into csv file
for k, v in custdata.collect()
  unique, morn, aft, eve, night, mobile = v
  tot = morn + aft + eve + night

  # persist the data, in this case, write to a file
  with open('live_table.csv', 'wb') as csvfile:
    fwriter = csv.writer(csvfile, delimited = ' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fwriter.writerow(unique, morn, aft, eve, night, mobile)

    # do the same with the summary data
    with open('agg_table.csv', 'wb') as csvfile:
    fwriter = csv.writer(csvfile, delimter=' ', quotechar="|", quoting=csv.QUOTE_MINIMAL)
    fwriter.writerow(aggdata.mean()[0], aggdata.mean()[1], aggdata.mean()[2], aggdata.mean()[3], aggdata.mean()[4], aggdata.mean()[5])
