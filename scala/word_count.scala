import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object word_count{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val srcFile = "../SparkCourses_Scala/data/Book.txt"
    val lines = sc.textFile(srcFile)
    val words = lines.flatMap(normalizeWords)
    val wordsCount = words.map(map_words).reduceByKey(reduce_words)
    wordsCount.foreach(println)
  }

  def normalizeWords(lines:String): Array[String] = {
    val result = lines.split("\\W+")
    return result
  }
  def map_words(x:String) : (String, Int) = {
    return (x,1)
  }
  def reduce_words(x: Int, y: Int) : (Int) = {
    return x+y
  }
}
