import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.{abs, max, min}

object min_temperatures{
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("MinTemperatures")
    var sc = new SparkContext(conf)
    var srcFile = "../SparkCourses_Scala/data/1800.csv"
    var lines = sc.textFile(srcFile)
    val parsedLines = lines.map(parseLine)
    val minTemperature = parsedLines.filter(_._2 == "TMIN")
    val stationTemps = minTemperature.map(map_stationTemps)
    val minTemp = stationTemps.reduceByKey(reduce_stationTemps)
    minTemp.foreach(PrintMinTemp)
  }

  def parseLine(x:String) : (String, String, Float) = {
    val fields = x.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = (fields(3).toFloat * 0.1 * (9.0 / 5.0) + 32.0).toFloat
    return (stationId, entryType, temperature)
  }

  def map_stationTemps(x:(String, String, Float)) : (String, Float) = {
    return (x._1, x._3)
  }

  def reduce_stationTemps(x:Float, y: Float) : Float = {
    return min(x,y)
  }

  def PrintMinTemp(data:(String, Float)) : Unit = {
    println("StationId : "+data._1)
    println("Temperature : " +data._2)
    println("#########################")
  }
}
