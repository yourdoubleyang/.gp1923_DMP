package util

import java.lang

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer


/**
  * 从高德获取商圈信息
  */
object AmapUtil {
  def getBusinessFromAmap(long: Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957
    //&key=42f5c594ff65aa88cf83117e1d026bfe&extensions=all
    val location = long+","+lat
    //获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=8b51e706ef7be8c8032ec24dd57e32bf&extensions=all"
    //调用http接口发送请求
    val jsonstr: String = HttpUtil.get(url)
    //解析json串
    val jsonobject: JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val statusid: Int = jsonobject.getIntValue("status")
    if(statusid==0) return ""
    //如果不为空，解析商圈信息
    val json1: JSONObject = jsonobject.getJSONObject("regeocode")
    if(json1==null) return ""
    val json2: JSONObject = json1.getJSONObject("addressComponent")
    if(json2==null) return ""
    val array = json2.getJSONArray("businessAreas")
    if(array == null) return ""
    //定义集合取值
    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    //循环数组，返回每一个商圈name
    for(item<- array.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }

    //得到商圈名字
    result.mkString(",")

  }
}
