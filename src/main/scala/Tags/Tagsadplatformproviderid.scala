package Tags


import org.apache.spark.sql.Row
import util.Tag


object Tagsadplatformproviderid extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val adchannel: Int = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+adchannel,1)
    list
  }
}
