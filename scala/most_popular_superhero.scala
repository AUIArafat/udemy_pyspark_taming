import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object most_popular_superhero {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MostPopularSuperHero")
    val sc = new SparkContext(conf)
    val srcFile = "../SparkCourses_Scala/data/Marvel-Names.txt"
    val names = sc.textFile(srcFile)
    val namesRdd = names.map(map_ParseLine)


    val lines = sc.textFile("../SparkCourses_Scala/data/Marvel-Graph.txt")
    val pairings = lines.map(CountCoOccurances)
    val totalFriendsByHero = pairings.reduceByKey(reducer)
    val flipped = totalFriendsByHero.map(swap)
    val mostPopular = flipped.max()
    val mostPopularNames = namesRdd.lookup(mostPopular._2)(0)
    println(mostPopularNames)
  }

  def map_ParseLine(lines:String) : (Int, String) = {
    val fields = lines.split(" ");
    var name="";
    var i=1;
    while(i<fields.length)
      {
        name=name+" "+fields(i);
        i=i+1;
      }
    return  (fields(0).trim().toInt, name.replace("\"",""))
  }

  def CountCoOccurances(x:String) : (Int, Int) = {
    val fields = x.split(" ")
    return (fields(0).toInt, fields.length-1)
  }

  def reducer(x:Int, y:Int):(Int) = {
    return x+y
  }
  def swap(x:(Int, Int)) : (Int, Int) = {
    return (x._2, x._1)
  }
}
