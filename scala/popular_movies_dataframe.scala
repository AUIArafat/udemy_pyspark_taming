import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source
object popular_movies_dataframe{
  var movieNames = Map[Int, String]()
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)
    var spark = new SparkSession.Builder().master("local").appName("PopularMoviesDataFrame").getOrCreate()
    var nameDict = loadMovieNames()
    val lines = spark.sparkContext.textFile("../SparkCourses_Scala/data/u.data")
    val movies = lines.map(map_movies)
    movies.foreach(println)
    var schema = List(
      StructField("MovieId", IntegerType, true)
    )
    var movieDataset = spark.createDataFrame(movies, StructType(schema)).cache()
    var topMovieIds = movieDataset.groupBy("MovieId").count().orderBy(desc("count")).cache()
    topMovieIds.show()
    var top10 = topMovieIds.take(10)
    for(movie<-top10){
      println(movieNames(movie(0).toString.toInt) + " : " + movie(1))
    }
  }

  def loadMovieNames() : Map[Int, String] = {
    val srcFile = "../SparkCourses_Scala/data/u.item"
    val movieData = Source.fromFile(srcFile, enc = "ISO-8859-1")
    for (lines<-movieData.getLines()){
      var fields = lines.split("[|]")
      movieNames = movieNames + (fields(0).toInt->fields(1))
    }
    return movieNames
  }

  def map_movies(lines:String):Row = {
    var fields = lines.split("\\s")
    return Row(fields(1).toInt)
  }

}