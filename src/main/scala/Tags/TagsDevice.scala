package Tags

import org.apache.spark.sql.Row
import util.Tag

object TagsDevice extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    //设备操作系统
    val client: Int = row.getAs[Int]("client")
    client match {
      case t if t == 1 =>  list:+=("Android D00010001",1)
      case t if t == 2 =>  list:+=("IOS D00010002",1)
      case t if t == 3 =>  list:+=("WinPhone D00010003",1)
      case _ => list:+=("其 他 D00010004",1)
    }

    //设备联网方式
    val networkmannername = row.getAs[String]("networkmannername")
    networkmannername match {
      case t if t == "2G" => list:+=("2G D00020004",1)
      case t if t == "3G" => list:+=("3G D00020003",1)
      case t if t == "4G" => list:+=("4G D00020002",1)
      case t if t == "WIFI" => list:+=("WIFI D00020001",1)
      case _ => list:+=("_   D00020005",1)
    }

    //设备运营商方式
    val ispname = row.getAs[String]("ispname")
    ispname match {
      case t if t == "移动" => list:+=("移 动 D00030001",1)
      case t if t == "联通" => list:+=("联 通 D00030002",1)
      case t if t == "电信" => list:+=("电 信 D00030003",1)
      case _ => list:+=("_ D00030004",1)
    }

    list
  }
}
