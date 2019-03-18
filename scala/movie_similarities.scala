import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.math.sqrt

import scala.io.Source

object movie_similarities{
  var scoreThreshold = 0.97
  var coOccurenceThreshold = 30
  var movieID = 61
  var movieDict = Map[Int, String]()
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local").setAppName("MovieSimilarities")
    val sc = new SparkContext(conf)
    println("Loading Movie Names...")
    var nameDict = loadMovieNames()
    val data = sc.textFile("../SparkCourses_Scala/data/u.data")
    val ratings = data.map(map_ratings)
    //ratings.foreach(println)
    val joinedRatings = ratings.join(ratings)
    //joinedRatings.foreach(println)
    val uniqueJoinedRatings = joinedRatings.filter(filterUniqueRatings)
    //uniqueJoinedRatings.foreach(println)
    val moviePairs = uniqueJoinedRatings.map(makePair)
    //moviePairs.foreach(println)
    var moviePairRatings = moviePairs.groupByKey()
    var moviePariSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    var filteredResults = moviePariSimilarities.filter(pairSimilarity)

    var results = filteredResults.map(pairSim => (pairSim._2, pairSim._1)).sortByKey(false, 10)

    print("Top 10 similar movies for " + nameDict(movieID))
    results.foreach(printSimilarMovies)
  }

  def loadMovieNames() : Map[Int, String] = {
    val srcFile = "../SparkCourses_Scala/data/u.item"
    val movieData = Source.fromFile(srcFile, enc = "ISO-8859-1")
    for(line<-movieData.getLines()){
      val fields = line.split("[|]")
      movieDict += (fields(0).toInt->fields(1))
    }
    return movieDict
  }

  def map_ratings(line:String):(Int, (Int, Float))={
    val fields = line.split("\t");
    return (fields(0).toInt, (fields(1).toInt, fields(2).toFloat))
  }

  def filterUniqueRatings(userRatings:(Int, ((Int,Float), (Int, Float)))) : Boolean={
    var movie1:Int = userRatings._2._1._1
    var movie2:Int = userRatings._2._2._1
    //println(movie1 + " user " + movie2)
    return movie1<movie2
  }

  def makePair(usingRatings:(Int, ((Int,Float), (Int, Float)))) : ((Int, Int), (Float, Float)) = {
    var ratings = usingRatings._2
    var movie1 = ratings._1._1
    var movie2 = ratings._2._1
    var rating1 = ratings._1._2
    var rating2 = ratings._2._2
    return ((movie1, movie2), (rating1, rating2))
  }

  def computeCosineSimilarity(ratingPairs:Iterable[(Float, Float)]) : (Float, Int) = {
    var numPairs = 0
    var sum_xx = 0.0
    var sum_yy = 0.0
    var sum_xy = 0.0

    ratingPairs.foreach(f=>{
      sum_xx = sum_xx + f._1 *f._1
      sum_yy = sum_yy + f._2*f._2
      sum_xy = sum_xy + f._1*f._2
      numPairs+=1
    })
    var numerator = sum_xy
    var denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score = 0.0
    if (denominator>0) {
      score = (numerator.toFloat / (denominator.toFloat))
    }

    return(score.toFloat, numPairs)
  }

  def pairSimilarity(pairSim:((Int, Int),(Float, Int))):Boolean={
    if((pairSim._1._1 == movieID || pairSim._1._2 == movieID) &&
    (pairSim._2._1 > scoreThreshold && pairSim._2._2 > coOccurenceThreshold)){
      return true
    }
    return  false
  }

  def printSimilarMovies(movies:((Float, Int), (Int, Int))){
    var sim:(Float, Int) = movies._1
    var pair:(Int, Int) = movies._2
    var similarMovieID = pair._1
    if(similarMovieID == movieID){
      similarMovieID = pair._2
    }
    println(movieDict(similarMovieID) + " score: " + sim._1 + " strength: " + sim._2)
  }
}