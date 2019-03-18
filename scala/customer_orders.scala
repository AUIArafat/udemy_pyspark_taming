import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object customer_orders{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CustomerOrder")
    val sc = new SparkContext(conf)
    val srcFile = "../SparkCourses_Scala/data/customer-orders.csv"
    val lines = sc.textFile(srcFile)
    val parsedLines = lines.map(map_parseLine).reduceByKey(reduce_parseLine)
    val sortedResult = parsedLines.map(swap).sortByKey()
    sortedResult.foreach(Print)
  }

  def map_parseLine(x:String) : (Int, Float) = {
    val fields = x.split(",")
    val customer_id = fields(0).toInt
    val amount = fields(2).toFloat
    return (customer_id, amount)
  }

  def reduce_parseLine(x:Float, y:Float) : (Float) = {
    return x+y
  }

  def swap(x:(Int, Float)) : (Float, Int) = {
    return (x._2, x._1)
  }

  def Print(data:(Float, Int)) : Unit = {
    println("CustomerId : "+data._2)
    println("Amount : " +data._1)
    println("#########################")
  }
}
