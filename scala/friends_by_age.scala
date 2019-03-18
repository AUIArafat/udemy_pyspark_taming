
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object friends_by_age {
  def main(args : Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("FriendsByAge")
    val sc = new SparkContext(conf)
    val srcFile ="../SparkCourses_Scala/data/fakefriends.csv"
    val lines = sc.textFile(srcFile)
    val rdd = lines.map(parseLine)
    val totalByAge = rdd.mapValues(map_friends_age).reduceByKey(reduce_friends_age)
    val avgByAge = totalByAge.mapValues(map_avg_age).sortByKey()
    avgByAge.foreach(println)
  }

  def parseLine(x:String) : (Int, Int) = {
    val fields = x.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    return (age, numFriends)
  }

  def map_friends_age(x:Int) : (Int, Int) = {
    println(x)
    return (x,1)
  }

  def reduce_friends_age(x:(Int,Int), y:(Int, Int)) : (Int, Int) = {
    return (x._1 + y._1, x._2+y._2)
  }

  def map_avg_age(x:(Int, Int)): Float = {
    return roundUp(x._1.toFloat/x._2.toFloat)
  }

  def roundUp(d: Double) = math.floor(d).toInt
}