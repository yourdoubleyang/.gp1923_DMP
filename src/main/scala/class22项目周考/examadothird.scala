package class22项目周考

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object examadothird {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
     val src: DataFrame = sparkSession.read.text("in/json.txt")
     val aa : DataFrame = sparkSession.read.json("in/json.txt")
    src.rdd.foreach(println)
    aa.rdd.foreach(println)


  }
}
