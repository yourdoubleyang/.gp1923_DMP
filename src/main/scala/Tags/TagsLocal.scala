package Tags

import org.apache.spark.sql.Row
import util.Tag

object TagsLocal extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val province: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")

    list:+=("ZP"+province,1)
    list:+=("ZC"+city,1)

    list
  }
}
