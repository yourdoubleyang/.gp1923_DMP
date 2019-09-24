package Tags

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




/**
  * 上下文标签主类
  */
object TagsContext {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath,day) = args

    //创建spark上下文
    val sparkSession = SparkSession.builder()
      .appName("Tags")
      .master("local[*]")
      .getOrCreate()

    //读取数据文件
    val src: DataFrame = sparkSession.read.parquet(inputPath)

    val src1: DataFrame = sparkSession.read.text("in/app_dict.txt")

//    //调用Hbase api
//    //加载配置文件
//    val load: Config = ConfigFactory.load()
//    //获取表名
//    val HbaseTableName: String = load.getString("HBASE.tablename")
//    //创建一个hadoop任务
//    val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration
//    //配置Hbase连接
//    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
//    //获取connection连接
//    val hbconn: Connection = ConnectionFactory.createConnection(configuration)
//    val hbadmin: Admin = hbconn.getAdmin
//    //判断当前表是否被使用
//    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
//      println("当前表可用")
//      //创建表对象
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
//      //创建列簇
//      val hColumnDescriptor = new HColumnDescriptor("tags")
//      //将创建好的列簇加入表中
//      tableDescriptor.addFamily(hColumnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbconn.close()
//    }
//    val conf = new JobConf(configuration)
//    //指定输出类型
//    conf.setOutputFormat(classOf[TableOutputFormat])
//    //指定输出哪张表
//    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)



    import sparkSession.implicits._
    val rdd1: Dataset[(String, String)] = src1.map(line => {
      line.getString(0).split("//s")
    }).filter(_.length >= 5).map(x => {
      (x(4), x(1))
    })
    val map: Map[String, String] = rdd1.collect().toMap

    val broadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)

    //处理数据信息
    import sparkSession.implicits._
    val res = src.rdd.map(line => {
      //获取用户的唯一ID
      val userId: String = util.TagsUtil.getOneUserId(line)
      //接下来标签的实现
      //广告标签的实现
      val tagsAd: List[(String, Int)] = TagsAd.makeTags(line)
      //渠道标签的实现
      val tagsAdchannel: List[(String, Int)] = Tagsadplatformproviderid.makeTags(line)
      //app名称的实现
      val tagsAppname: List[(String, Int)] = TagsAppName.makeTags(line, broadcast)

      //设备指标实现
      val tagsClient: List[(String, Int)] = TagsDevice.makeTags(line)

      //关键字
      val tagsKeyWords: List[(String, Int)] = TagsKeyWords.makeTags(line)

      //地域标签
      val tagslocal: List[(String, Int)] = TagsLocal.makeTags(line)

      //商圈的标签的实现
      val tagsbusiness: List[(String, Int)] = TagsBusiness.makeTags(line)

      (userId, (tagsAd++tagsAdchannel++tagsAppname++tagsClient++tagsKeyWords++tagslocal++tagsbusiness))

    })
    res.foreach(println)



    val res10: RDD[(String, List[(String, Int)])] = res.reduceByKey((list1, list2) => {
      val list3: List[(String, Int)] = list1 ::: list2
      val grouped: Map[String, List[(String, Int)]] = list3.groupBy(_._1)
      val resfinal: Map[String, Int] = grouped.mapValues(_.size)
      resfinal.toList
    })

    //res10.foreach(println)
//    res10.map{
    ////      case(userId,userTags) =>{
    ////        //设置rowkey和列，列名
    ////        val put = new Put(Bytes.toBytes(userId))
    ////        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
    ////        (new ImmutableBytesWritable(),put)
    ////      }
    ////    }.saveAsHadoopDataset(conf)

//    val res11: RDD[(String, List[(String, Int)])] = res.reduceByKey((list1, list2) => {
//      val list3: List[(String, Int)] = list1 ::: list2
//      val grouped: Map[String, List[(String, Int)]] = list3.groupBy(_._1)
//      val mapved: Map[String, Int] = grouped.mapValues(_.foldLeft(0)(_+_._2))
//      mapved.toList
//    })
//
//    res11.foreach(println)










  }


}
