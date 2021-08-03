import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
/*
object task1 {

  def main(arg: Array[String]): Unit={
    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val input_file = "/Users/wentaozhou/desktop/test_review.json"
    val rdd = sc.textFile(path = input_file)
    val textRDD = rdd.map{ row => val json_row = jsonStrToMap(row)
      (json_row)
    }
    val newRDD = textRDD.map(row => (row("user_id"), row("business_id"), row("date")))
    val count = newRDD.count()
   // val review2018 =  textRDD.filter(row => row("date").toString().contains("2018")).count()
    val review = newRDD.filter(row => row._3.toString.contains("2018")).count()
    val distinctuser = newRDD.map(row => row._1.toString()).distinct().count()
    val toptenusers = newRDD.map(row => (row._1,1)).reduceByKey((a,b) => a+b)
      .sortBy(row => (-row._2.toInt, row._1.toString())).take(10)
    val numdistinctbusineess = newRDD.map(row => row._2).distinct().count()
    val toptenbusiness = newRDD.map(row => (row._2,1)).reduceByKey((a,b) => a+b).sortBy(row => (-row._2.toInt, row._1.toString())).take(10)
    val json = ("n_review" -> count) ~ ("n_review_2018" -> toptenusers)
    print(compact(render(json)))
  }



  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }
}
*/