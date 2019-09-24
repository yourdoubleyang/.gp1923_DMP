package class22项目周考

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object examaboutTags {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val src: RDD[String] = sparkContext.textFile("in/json.txt")

    val rdd1: RDD[ListBuffer[String]] = src.map(line => {
      val list: ListBuffer[String] = ExamUtil.getfields(line)
      list
    })



    val rdd2: RDD[String] = rdd1.flatMap(x=>x)

    val rdd3: RDD[(String, Iterable[String])] = rdd2.groupBy(x=>x)

    val res2: RDD[(String, Int)] = rdd3.mapValues(_.size)

    res2.foreach(println)
  }
}
