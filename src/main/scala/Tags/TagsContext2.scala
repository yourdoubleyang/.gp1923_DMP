package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TagsContext2 {
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

    val mappedSrc= src.rdd.map(row => {
      val userId: List[String] = util.TagsUtil.getallUserId(row)
      (userId, row)
    })


    //构建点集合
    val verties= mappedSrc.flatMap(row => {
      val line: Row = row._2
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

      val tagList: List[(String, Int)] = tagsAd ++ tagsAdchannel ++ tagsAppname ++ tagsClient ++ tagsKeyWords ++ tagslocal

      //下面这步得到的是这样子的结果：List((uid1,0),(uid2,0),(uid3,0),(uid4,0),(标签1,1),(标签2,1),(标签3,1),(标签4,1),(标签5,1))
      val VD: List[(String, Int)] = row._1.map((_, 0)) ++ tagList

      //保留所有的用户ID

      //1，如果保证其中一个ID携带用户的标签
      //2.用户ID的字符串怎么处理
      row._1.map(uid => {
        if (row._1.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })


    })

    //verties.take(20).foreach(println)


    //构建边的集合
    val edges: RDD[Edge[Int]] = mappedSrc.flatMap(row => {
      row._1.map(uid => Edge(row._1.head.hashCode.toLong, uid.hashCode.toLong, 0))
    })
    //edges.foreach(println)



    //构建图
    val graph = Graph(verties,edges)

    //根据图计算中的连通图算法，通过图中的分支，连通所有的点
    //然后再根据所有的点，找到内部最小的点，为当前的公共点

    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //聚合所有的标签
    vertices.join(verties).map{
      case (uidd,(cnId,tagsAndUserID))=>{
        (cnId,tagsAndUserID)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    }).foreach(println)

  }
}
