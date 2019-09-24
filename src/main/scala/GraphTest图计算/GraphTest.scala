package GraphTest图计算

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphTest {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()
    //创建点和边
    //构建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sparkSession.sparkContext.makeRDD(Seq(
      (1L, ("小明", 26)),
      (2L, ("小红", 30)),
      (6L, ("小黑", 33)),
      (9L, ("小常", 26)),

      (133L,("小时",37)),
      (138L,("小小",30)),
      (158L,("小端",30)),

      (16L, ("小王", 33)),
      (21L, ("小都", 26)),
      (44L, ("小主", 30)),

      (5L, ("小大", 33)),
      (7L, ("小打", 26))
    ))

    //构建边的集合
    val edgeRDD: RDD[Edge[Int]] = sparkSession.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),

      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),

      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构建图
    val graph = Graph(vertexRDD,edgeRDD)


    //取顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.foreach(println)


    //匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_++_)
      .foreach(println)
  }
}
