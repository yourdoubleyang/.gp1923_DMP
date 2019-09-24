package util

import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object TestHttpUtil {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    val arr = Array("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=8b51e706ef7be8c8032ec24dd57e32bf&extensions=all")

    val rdd = sparkSession.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HttpUtil.get(t)
    })
      .foreach(println)
  }
}
