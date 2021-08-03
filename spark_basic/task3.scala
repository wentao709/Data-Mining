/*import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object task3 {
  def main(arg: Array[String]): Unit={
    val spark = new SparkConf().setAppName("task3").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val input_file = "/Users/wentaozhou/desktop/test_review.json"
    val input_file2 = "/Users/wentaozhou/desktop/business_review.json"
    val rdd = sc.textFile(path = input_file)
    val rdd2 = sc.textFile(path = input_file2)
    val textRDD = rdd.map{ row => val json_row = jsonStrToMap(row)
      (json_row)
    }
    val business = rdd2.map{ row => val json = jsonStrToMap(row)
      (json)
    }
    val text = textRDD.map(row => (row("business_id"),row("star")))
    val busi = business.map(row => (row("business_id"),row("city")))
    val joined = busi.join(text)
   // val j = joined.map(row => row).groupByKey().mapValues()

  }
  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }

}

*/