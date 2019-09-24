package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spire.std.map
import util.Tag

object TagsAppName extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]
//    val sparkSession:SparkSession = args(1).asInstanceOf[SparkSession]
//    val src1: DataFrame = sparkSession.read.text("in/app_dict.txt")
//    //src1.show()
//    import sparkSession.implicits._
//    val ds1: Dataset[(String, String)] = src1.map(line => {
//        val arr = line.getString(0).split("\\s",-1)
//      arr
//    }).filter(_.length >= 5).map(x => {
//      (x(4), x(1))
//    })
//    ds1.show()
//    val map: Map[String, String] = ds1.collect().toMap

    val broadcast: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var aaa=""
    if(StringUtils.isBlank(row.getAs[String]("appname"))){
      val appid = row.getAs[String]("appid")
      val appname: String = broadcast.value.getOrElse("appid","unknow")
      aaa=appname
    }else{
      val appname: String = row.getAs[String]("appname")
      aaa=appname
    }
    list:+=("APP"+aaa,1)

    list
  }
}
