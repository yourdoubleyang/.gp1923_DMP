package class22项目周考

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object examdosecondmethod {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sparkContext = new SparkContext(sparkConf)
    val src: RDD[String] = sparkContext.textFile("in/json.txt")

    val rdd1: RDD[List[String]] = src.map(line => {
      var list = List[String]()
      val json1: JSONObject = JSON.parseObject(line)
      if (json1.getString("status") == "1") {
        val json2: JSONObject = json1.getJSONObject("regeocode")
        val array: JSONArray = json2.getJSONArray("pois")
        for (item <- array.toArray()) {
          val json1: JSONObject = JSON.parseObject(item.toString)
          val str: String = json1.getString("businessarea")
          list :+= (str)
        }
      }
      list
    })

    val res: RDD[(String, Int)] = rdd1.flatMap(x=>x).groupBy(x=>x).mapValues(_.size)

    res.foreach(println)
  }
}
