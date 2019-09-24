package Tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import util.{AmapUtil, JedisConnectionPool, String2Type, Tag}

/**
  * 商圈标签
  */
object TagsBusiness extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val long: String = row.getAs[String]("lat")
    val lat: String = row.getAs[String]("long")

    if(String2Type.toDouble(long)>=73
      &&String2Type.toDouble(long) <= 136
      &&String2Type.toDouble(lat) >=3
      &&String2Type.toDouble(lat) <= 53){
      //获取到周围所有的商圈名称
      val business: String = getBusiness(long.toDouble,lat.toDouble)
      if(StringUtils.isNoneBlank(business)){
        val arr: Array[String] = business.split(",")
        arr.foreach(arr=>{
          list:+=(arr,1)
        })
      }
    }
    list
  }
  //获取商圈信息
  def getBusiness(long:Double,lat:Double):String={
    //GeoHash码
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //数据库查询当前商圈的信息
    val business: String = redis_queryBusiness(geohash)
    //如果在已有的redis数据库里没有信息，则去高德
    if(business == null){
      val business: String = AmapUtil.getBusinessFromAmap(long,lat)
      //并将高德获取到的商圈存储于数据库中
      if(business != null && business.length >0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }
  //获取数据库里的商圈信息
  def redis_queryBusiness(geohash:String):String={
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val business: String = jedis.get(geohash)
    jedis.close()
    business
  }

  def redis_insertBusiness(geohash:String,business:String)={
    val jedis: Jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
