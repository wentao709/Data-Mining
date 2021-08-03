import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
/*
object task2 {
  def main(arg: Array[String]): Unit={
    val input_partition = 5
    val spark = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val input_file = "/Users/wentaozhou/desktop/test_review.json"
    val rdd = sc.textFile(path = input_file)
    val textRDD = rdd.map{ row => val json_row = jsonStrToMap(row)
      (json_row)
    }
    val t1 = System.nanoTime
    val toptenbusiness = textRDD.map(row => List(row("business_id"),1)).reduceByKey((a,b) => a+b)
    val so = toptenbusiness.sortBy(row => (-row._2.toInt, row._1.toString())).take(10)
    val duration = (System.nanoTime - t1) / 1e9d
    val numofpartition = toptenbusiness.getNumPartitions
    val itemperpartition = toptenbusiness.glom().map(x => x.size).collect()
    println(numofpartition)
    itemperpartition.foreach(println)
    print(duration)
    val t2 = System.nanoTime
    val topten_custom = textRDD.map(row => (row("business_id"),1)).reduceByKey((a,b) => a+b, input_partition)
    val so2 = toptenbusiness.sortBy(row => (-row._2.toInt, row._1.toString())).take(10)
    val duration2 = (System.nanoTime - t2) / 1e9d
    print(duration2)

  }



  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }
}
*/