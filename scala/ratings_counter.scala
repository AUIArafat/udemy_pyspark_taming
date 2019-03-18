import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ratings_counter {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("RatingsCounter")
    val sc = new SparkContext(conf)
    val srcFiles = "../SparkCourses_Scala/data/u.data"
    val lines = sc.textFile(srcFiles)
    val result = lines.map(ratings).reduceByKey(reducer).sortByKey()
    result.foreach(println)
  }
  def ratings(x:String) : (Int, Int) = {
    val fields = x.split("\t")
    return (fields(2).toInt, 1)
  }

  def reducer(x:Int, y:Int) : Int = {
    return x+y;
  }
}