package Tags

import org.apache.spark.sql.Row
import util.Tag

object TagsKeyWords extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val keywords: String = row.getAs[String]("keywords")

    if(keywords.length >=3 && keywords.length <= 8 && keywords.contains("|")==false){
      list:+=("K"+keywords,1)
    }else if(keywords.length >=3 && keywords.length <= 8 && keywords.contains("|")==true){
      val arr: Array[String] = keywords.split("|")
      for(item <- arr){
        if(item.length>=3 && item.length <= 8){
          list:+=("K"+item,1)
        }
      }
    }else{
      list:+=("K"+"没有关键字",1)
    }



    list
  }
}
