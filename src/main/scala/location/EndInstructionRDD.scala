package location

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object EndInstructionRDD {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录输入不对")
      sys.exit()
    }
    val Array(inputPath)=args
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val src: DataFrame = sparkSession.read.parquet(inputPath)
    val rdd1: RDD[(String, List[Double])] = src.rdd.map(line => {
      val requestmode = line.getAs[Int]("requestmode")
      val processnode = line.getAs[Int]("processnode")
      val iseffective = line.getAs[Int]("iseffective")
      val isbilling = line.getAs[Int]("isbilling")
      val isbid = line.getAs[Int]("isbid")
      val iswin = line.getAs[Int]("iswin")
      val adorderid = line.getAs[Int]("adorderid")
      val winprice = line.getAs[Double]("winprice")
      val adpayment = line.getAs[Double]("adpayment")
      val ispname = line.getAs[String]("ispname")

      val repqt: List[Double] = util.AreaDistributed.reqPt(requestmode, processnode)
      val clickpt: List[Double] = util.AreaDistributed.clickPt(requestmode, iseffective)
      val adpt: List[Double] = util.AreaDistributed.adPt(iseffective, isbid, isbilling, iswin, adorderid, winprice, adpayment)
      val networkmanner: String = util.AreaDistributed.getnetwork(ispname)
      val list = repqt ++ clickpt ++ adpt
      (networkmanner, list)
    })
    val rdd2: RDD[(String, List[Double])] = rdd1.reduceByKey((list1, list2) => {
      val list3: List[(Double, Double)] = list1.zip(list2)
      list3.map(x => x._2 + x._1)
    })
    val res: RDD[String] = rdd2.map(x => {
      x._1 + "," + x._2.mkString(",") + "," + x._2(4) / x._2(3) + "," + x._2(6) / x._2(5)
    })
    res.saveAsTextFile("D://bbb.txt")
  }
}
