import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._

import scala.collection.mutable.ArrayBuffer

object degrees_of_separation{
  val startCharacterId = 5306
  val targetCharacterId = 14
  val conf = new SparkConf().setMaster("local[*]").setAppName("DegreesOfSeparation")
  val sc = new SparkContext(conf)
  var hitCounter = new LongAccumulator()
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    hitCounter = (sc.longAccumulator("Hit Counter"))
    hitCounter.add(0)
    var iterationRdd = createStartingRdd(sc)
    var iteration:Int = 0
    for (iteration <-1 to 10){
      println("Running BFS Iteration# " + iteration)
      val mapped = iterationRdd.flatMap(bfsMap)
      println("Processing " + mapped.count() + " values. " + hitCounter.value)

      if(hitCounter.value>0){
        val hitCount = hitCounter.value
        println(hitCount + "Ha Ha Ha")
        if(hitCount>0){
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }

  def createStartingRdd(sc:SparkContext) : RDD[(Int, (Array[Int], String, Int))] = {
    val inputFile = sc.textFile("../SparkCourses_Scala/data/Marvel-Graph.txt")
    return inputFile.map(convertToBFS)
  }

  def convertToBFS(line:String) : (Int, (Array[Int], String, Int)) = {
    val fields = line.split("\\s")
    val heroID = fields(0).toInt
    var connections : ArrayBuffer[Int] = ArrayBuffer()
    for(connection <-1 to (fields.length-1)){
      connections += fields(connection).toInt
    }

    var color: String = "WHITE"
    var distance: Int = 9999
    if(heroID == startCharacterId){
      color = "GRAY"
      distance = 0
    }
      return (heroID, (connections.toArray, color, distance))
  }

  def bfsMap(node : (Int, (Array[Int], String, Int))) : Array[(Int, (Array[Int], String, Int))] = {
    val characterID:Int = node._1
    val data:(Array[Int], String, Int)  = node._2
    val connections: Array[Int] = data._1
    val distance:Int = data._3
    var color:String = data._2
    var results:ArrayBuffer[(Int, (Array[Int], String, Int))] = ArrayBuffer()
    if(color == "GRAY"){
      for(connection <- connections){
        val newCharacterID = connection
        val newDistance = distance+1;
        val newColor = "GRAY"
        //println("New : " + targetCharacterId + " con : " + connection)
        if(targetCharacterId == connection){
          //if(hitCounter.value>0){
            hitCounter.add(1)
          //}
        }
        val newEntry:(Int, (Array[Int], String, Int)) = (newCharacterID, (Array(), newColor, newDistance))
        results +=newEntry
      }
      color = "BLACK"
    }
    val thisEntry:(Int, (Array[Int], String, Int)) = (characterID, (connections, color, distance))
    results += thisEntry
    return  results.toArray
  }

  def bfsReduce(data1:(Array[Int], String, Int), data2:(Array[Int], String, Int)) : (Array[Int], String, Int) = {
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._3
    val distance2:Int = data2._3
    val color1:String = data1._2
    val color2:String = data2._2

    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()


    if(edges1.length > 0){
      edges ++= edges1
    }

    if(edges2.length > 0){
      edges ++= edges2
    }

    if(distance1<distance){
      distance = distance1
    }

    if(distance2<distance){
      distance = distance2
    }

    if(color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")){
      color = color2
    }
    if(color1 == "GRAY" && color2 == "BLACK"){
      color = color2
    }
    if(color2 == "WHITE" && (color1 == "GRAY" && color1 == "BLACK")){
      color = color1
    }
    if(color2 == "GRAY" && color1 == "BLACK"){
      color = color1
    }
    return (edges.toArray, color, distance)
  }
}
