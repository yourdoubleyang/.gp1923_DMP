package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.Tag


object TagsAd extends Tag {
  override def makeTags(args: Any*) = {
    var list = List[(String, Int)]()

    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取广告位类型和名称
    val adType: Int = row.getAs[Int]("adspacetype")
    //广告位类型标签
    adType match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0" + v, 1)
    }

    //获取广告名称
    val adTypename: String = row.getAs[String]("adspacetypename")

    if(StringUtils.isBlank(adTypename)){
      list:+=("LN"+adTypename,1)
    }

    list
  }
}
