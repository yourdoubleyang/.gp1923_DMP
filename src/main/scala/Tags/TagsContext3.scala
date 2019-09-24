package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath) = args

    //创建spark上下文
    val sparkSession = SparkSession.builder()
      .appName("Tags")
      .master("local[*]")
      .getOrCreate()

    //读取数据文件
    val src: DataFrame = sparkSession.read.parquet(inputPath)

    val src1: DataFrame = sparkSession.read.text("in/app_dict.txt")
    import sparkSession.implicits._
    val rdd1: Dataset[(String, String)] = src1.map(line => {
      line.getString(0).split("//s")
    }).filter(_.length >= 5).map(x => {
      (x(4), x(1))
    })
    val map: Map[String, String] = rdd1.collect().toMap
    val broadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)

//        //调用Hbase api
//        //加载配置文件
//        val load: Config = ConfigFactory.load()
//        //获取表名
//        val HbaseTableName: String = load.getString("HBASE.tablename")
//        //创建一个hadoop任务
//        val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration
//        //配置Hbase连接
//        configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
//        //获取connection连接
//        val hbconn: Connection = ConnectionFactory.createConnection(configuration)
//        val hbadmin: Admin = hbconn.getAdmin
//        //判断当前表是否被使用
//        if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
//          println("当前表可用")
//          //创建表对象
//          val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
//          //创建列簇
//          val hColumnDescriptor = new HColumnDescriptor("tags")
//          //将创建好的列簇加入表中
//          tableDescriptor.addFamily(hColumnDescriptor)
//          hbadmin.createTable(tableDescriptor)
//          hbadmin.close()
//          hbconn.close()
//        }
//        val conf = new JobConf(configuration)
//        //指定输出类型
//        conf.setOutputFormat(classOf[TableOutputFormat])
//        //指定输出哪张表
//        conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    val mappedSrc: RDD[(List[String], Row)] = src.rdd.map(row => {
      val userList: List[String] = util.TagsUtil.getallUserId(row)
      (userList, row)
    })


    //构建点集合
    val verties: RDD[(Long, List[(String, Int)])] = mappedSrc.flatMap(line => {
      val row: Row = line._2
      val userList: List[String] = line._1


      //广告标签的实现
      val tagsAd: List[(String, Int)] = TagsAd.makeTags(row)
      //渠道标签的实现
      val tagsAdchannel: List[(String, Int)] = Tagsadplatformproviderid.makeTags(row)
      //app名称的实现
      val tagsAppname: List[(String, Int)] = TagsAppName.makeTags(row, broadcast)
      //设备指标实现
      val tagsClient: List[(String, Int)] = TagsDevice.makeTags(row)
      //关键字
      val tagsKeyWords: List[(String, Int)] = TagsKeyWords.makeTags(row)
      //地域标签
      val tagslocal: List[(String, Int)] = TagsLocal.makeTags(row)
      //商圈标签
      val tagsbusiness: List[(String, Int)] = TagsBusiness.makeTags(line)

      val TagsList = tagsAd++tagsAdchannel++tagsAppname++tagsClient++tagsKeyWords++tagslocal++tagsbusiness


      val VD: List[(String, Int)] = userList.map((_, 0)) ++ TagsList

      userList.map(uid => {
        if (userList.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })




    //构建边集合
    val edges: RDD[Edge[Int]] = mappedSrc.flatMap(line => {
      line._1.map(uid => Edge(line._1.head.hashCode.toLong, uid.hashCode.toLong, 0))
    })


    //构建图
    val graph = Graph(verties,edges)


    //找出最小点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val res0: RDD[(VertexId, List[(String, Int)])] = vertices.join(verties).map {
      case (uid, (cnid, tagsanduser)) =>
        (cnid, tagsanduser)
    }.reduceByKey((list1, list2) => {
      list1 ++ list2
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    })

    res0.foreach(println)

  }
}
