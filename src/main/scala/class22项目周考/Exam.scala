package class22项目周考

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer


object Exam {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sparkContext = new SparkContext(sparkConf)
    val src: RDD[String] = sparkContext.textFile("in/json.txt")


    val rdd1: RDD[ListBuffer[String]] = src.map(line => {
      val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
      val json: JSONObject = JSON.parseObject(line)
      if (json.getIntValue("status") == 1) {
        val json1: JSONObject = json.getJSONObject("regeocode")
        val arr: JSONArray = json1.getJSONArray("pois")
        for (item <- arr.toArray()) {
          if (item.isInstanceOf[JSONObject]) {
            val json = item.asInstanceOf[JSONObject]
            val name = json.getString("businessarea")
            result.append(name)
          }
        }
      }
      result
    })

    //rdd1.foreach(println)

    val rdd2: RDD[String] = rdd1.flatMap(x => x)

    val grouped: RDD[(String, Iterable[String])] = rdd2.groupBy(x => x)

    val res1: RDD[(String, Int)] = grouped.mapValues(_.size)

    res1.foreach(println)




  }
}
