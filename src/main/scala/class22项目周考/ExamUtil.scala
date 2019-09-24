package class22项目周考

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object ExamUtil {
  def getfields(args:Any*):ListBuffer[String]={
    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    val json: JSONObject = JSON.parseObject(args(0).toString)
    if (json.getIntValue("status") == 1) {
      val json1: JSONObject = json.getJSONObject("regeocode")
      val arr: JSONArray = json1.getJSONArray("pois")
      for (item <- arr.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          val typenames = json.getString("type")
          val arr: Array[String] = typenames.split(";")
          for(item <- arr){
            result.append(item)
          }
        }
      }
    }
    result
  }
}
