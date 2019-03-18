import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object spark_sql{
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder().master("local").appName("SparkSql").getOrCreate()
    val line = spark.sparkContext.textFile("../SparkCourses_Scala/data/fakefriends.csv")
    var people = line.map(mapper)
    people.foreach(println)
    var schema = List(
      StructField("Id",IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("NumberOfFriends", IntegerType, true)
    )
    var schemePeople = spark.createDataFrame(people, StructType(schema)).cache()
    for(i<-schemePeople.collect()){
      println(i)
    }
    schemePeople.createOrReplaceTempView("Pepole")
    var teenagers = spark.sql("SELECT *from Pepole where age >=13 AND age <=19")

    for(i<-teenagers.collect()){
      println(i)
    }

    schemePeople.groupBy("age").count().orderBy("age").show()
  }

  def mapper(x:String) : Row = {
    var fields = x.split(',')
    return Row(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }
}
