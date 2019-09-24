package location

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AreaDistributed地区分布 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    if(args.length != 1){
      println("文件目录输入错误")
      sys.exit()
    }
    val Array(inputPath)= args

    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val src: DataFrame = sparkSession.read.parquet(inputPath)

//    import sparkSession.implicits._
//    import org.apache.spark.sql.functions._
//    val res2: DataFrame = src.select($"provincename", $"cityname",
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1)as("original"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1)as("effective"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1)as("adrequest"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1)as("joincount"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" ===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1)as("successcount"),
//
//      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1)as("show"),
//      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1)as("click"),
//
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0)as("adpay"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0)as("adpprice")
//    )
//      .rollup("provincename", "cityname")
//      .agg(count("original").as("original"),
//        count("effective").as("effective"),
//      count("adrequest").as("adrequest"),
//      count("joincount").as("joincount"),
//      count("successcount").as("successcount"),
//      (count("successcount") / count("joincount")).as("successrate"),
//
//      count("show").as("show"),
//      count("click").as("click"),
//      (count("click") / count("show")).as("clickrate"),
//
//      sum("adpay"),
//      sum("adpprice"))
//        .orderBy($"provincename", $"cityname")
//
//    res2.show()




//    src.createTempView("arealog")

    //上钻的作用：本题来说是  先按省份和城市分组  再按省份分组

//    val res1: DataFrame = sparkSession.sql(
//      """
//select provincename,cityname,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE >= 1 then 1 else 0 end) as original,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE >= 2 then 1 else 0 end) as effective,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE = 3 then 1 else 0 end) as adrequest,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISBID = 1 then 1 else 0 end) as joincount,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) as successcount,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISBID = 1 then 1 else 0 end) / sum(case when ISEFFECTIVE = '1' and  ISBILLING = '1' and ISWIN = '1' and ADORDERID != 0 then 1 else 0 end) as successrate,
//sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) as show,
//sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) as click,
//sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end)/sum(case when REQUESTMODE = 2 and ISEFFECTIVE = '1' then 1 else 0 end) as clickrate,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then adpayment/1000 end) as adpay,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then WinPrice/1000 end) as adprice
//from arealog
//group by provincename,cityname with rollup
//
//order by provincename,cityname
//
//    """.stripMargin)
//    res1.show()
//    sparkSession.stop()

    val rdd1: RDD[((String, String), List[Double])] = src.rdd.map(line => {
      val requestmode = line.getAs[Int]("requestmode")
      val processnode = line.getAs[Int]("processnode")
      val iseffective = line.getAs[Int]("iseffective")
      val isbilling = line.getAs[Int]("isbilling")
      val isbid = line.getAs[Int]("isbid")
      val iswin = line.getAs[Int]("iswin")
      val adorderid = line.getAs[Int]("adorderid")
      val winprice = line.getAs[Double]("winprice")
      val adpayment = line.getAs[Double]("adpayment")
      val repqt: List[Double] = util.AreaDistributed.reqPt(requestmode, processnode)
      val clickpt: List[Double] = util.AreaDistributed.clickPt(requestmode, iseffective)
      val adpt: List[Double] = util.AreaDistributed.adPt(iseffective, isbid, isbilling, iswin, adorderid, winprice, adpayment)
      val list: List[Double] = repqt ++ clickpt ++ adpt
      val provincename = line.getAs[String]("provincename")
      val cityname = line.getAs[String]("cityname")
      ((provincename, cityname), list)
    })
      val rdd2: RDD[((String, String), List[Double])] = rdd1.reduceByKey((list1,list2)=>list1.zip(list2).map(x=>(x._1+x._2)))
    val res0: RDD[String] = rdd2.map(x => {
      x._1 + "," + x._2.mkString(",")+","+x._2(4)/x._2(3)+","+x._2(6)/x._2(5)
    })
    res0.foreach(println)

    res0.saveAsTextFile("D://aaaaa.txt")


  }
}

