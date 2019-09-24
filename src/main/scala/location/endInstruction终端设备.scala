package location


import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object endInstruction终端设备 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    if(args.length != 1){
      println("输入的目录不对")
      sys.exit()
    }
    val Array(inputPath) = args

    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val src: DataFrame = sparkSession.read.parquet(inputPath)


    //运营
//    import sparkSession.implicits._
//    import org.apache.spark.sql.functions._
//    val res2: DataFrame = src.select(
//      when($"ispname" === "电信", value = "电信")
//        when($"ispname" === "移动", value = "移动")
//        when($"ispname" === "联通", value = "联通")
//        when($"ispname" =!= "电信" and $"ispname" =!= "移动" and $"ispname" =!= "联通", value = "其他") as ("ispname"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1).as("original"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1).as( "effective"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1).as("adrequest"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1).as("joincount"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING"===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1).as("successcount"),
//
//      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1).as("show"),
//      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1).as("click"),
//
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0).as("adpay"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0).as("adpprice")
//    )
//      .groupBy("ispname")
//      .agg(count("original").as("original"),
//        count("effective").as("effective"),
//        count("adrequest").as("adrequest"),
//        count("joincount").as("joincount"),
//        count("successcount").as("successcount"),
//        (count("successcount") / count("joincount")).as("successrate"),
//
//        count("show").as("show"),
//        count("click").as("click"),
//        (count("click") / count("show")).as("clickrate"),
//
//        sum("adpay"),
//        sum("adpprice"))
//    res2.show()
//    sparkSession.stop()













//    src.createTempView("endinstrument")
//
//    val res2: DataFrame = sparkSession.sql(
//      """
//select
//newispname,
//sum(original)  as original,
//sum(effective) as effective,
//sum(adrequest) as adrequest,
//sum(joincount) as joincount,
//sum(successcount) as successcount,
//sum(successcount) / sum(joincount) as successrate,
//sum(show) as show,
//sum(click) as click,
//sum(click) / sum(show) as clickrate,
//sum(adpay),
//sum(adprice)
//
//
//from
//(select *,
//case when ispname != '电信' and  ispname != '联通' and  ispname != '移动' then '其他' else ispname end as newispname
//
//from
//
//(select ispname,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE >= 1 then 1 else 0 end) as original,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE >= 2 then 1 else 0 end) as effective,
//sum(case when REQUESTMODE = 1 and  PROCESSNODE = 3 then 1 else 0 end) as adrequest,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISBID = 1 then 1 else 0 end) as joincount,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) as successcount,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISBID = 1 then 1 else 0 end) / sum(case when ISEFFECTIVE = '1' and  ISBILLING = '1' and ISWIN = '1' and ADORDERID != 0 then 1 else 0 end) as successrate,
//sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) as show,
//sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) as click,
//sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end)/sum(case when REQUESTMODE = 2 and ISEFFECTIVE = '1' then 1 else 0 end) as clickrate,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then adpayment/1000.0 end) as adpay,
//sum(case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then WinPrice/1000.0 end) as adprice
//from endinstrument
//group by ispname))
//
//group by newispname
//
//    """.stripMargin)
//    res2.show()
//    sparkSession.stop()



//==============================================================================================================
//    网络类
//
//        import sparkSession.implicits._
//        import org.apache.spark.sql.functions._
//    src.select(
//      when($"networkmannername"==="2G","2G")
//        when($"networkmannername"==="3G","3G")
//        when($"networkmannername"==="4G","4G")
//        when($"networkmannername"==="wifi","wifi")
//        when($"networkmannername"=!="3G" and $"networkmannername"=!="4G" and $"networkmannername"=!="2G" and $"networkmannername"=!="wifi","其他") as "networkname",
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1).as("original"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1).as( "effective"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1).as("adrequest"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1).as("joincount"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING"===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1).as("successcount"),
//      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1).as("show"),
//      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1).as("click"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0).as("adpay"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0).as("adpprice")
//    )
//      .groupBy("networkname")
//      .agg(sum("original")as("original"),
//        sum("effective")as("effective"),
//        sum("adrequest")as("adrequest"),
//        sum("joincount")as("joincount"),
//        sum("successcount")as("successcount"),
//        sum("successcount")/sum("joincount") as "successrate",
//
//        sum("show")as("show"),
//        sum("click")as("click"),
//        sum("show")/sum("click") as "clickrate",
//
//        sum("adpay")as("adpay"),
//        sum("adpprice")as("adpprice")
//      ).show()






//    src.createTempView("t2")
//    sparkSession.sql(    """
//      |select networkname,
//      |sum(original),
//      |sum(effective),
//      |sum(adrequest),
//      |
//      |sum(joincount),
//      |sum(successcount),
//      |sum(successcount)/sum(joincount),
//      |
//      |sum(show),
//      |sum(click),
//      |sum(click)/sum(show),
//      |
//      |sum(adpay),
//      |sum(adprice)
//      |from
//      |
//      |(select
//      |case when networkmannername != '2G' and networkmannername != '3G' and networkmannername != '4G' and networkmannername != 'wifi'
//      |then "qita" else networkmannername end as networkname,
//      |case when REQUESTMODE = 1 and  PROCESSNODE >= 1 then 1 else 0 end as original,
//      |case when REQUESTMODE = 1 and  PROCESSNODE >= 2 then 1 else 0 end as effective,
//      |case when REQUESTMODE = 1 and  PROCESSNODE = 3 then 1 else 0 end as adrequest,
//      |case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISBID = 1 then 1 else 0 end as joincount,
//      |case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end as successcount,
//      |
//      |case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end as show,
//      |case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end as click,
//
//      |case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then adpayment/1000.0 end as adpay,
//      |case when ISEFFECTIVE = 1 and  ISBILLING = 1 and ISWIN = 1 then WinPrice/1000.0 end as adprice
//      |from t2)
//      |
//      |group by networkname
//      |
//      |
//      |
//    """.stripMargin
//    ).show()






//    =============================================================================================================
//    设备类
//    import org.apache.spark.sql.functions._
//    import sparkSession.implicits._
//    src.select(
//      when($"devicetype" === 1,"手机")
//        when($"devicetype"=== 2,"平板")
//        when($"devicetype" =!= 1 and $"devicetype"=!=2,"其他") as ("devicetype"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1).as("original"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1).as( "effective"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1).as("adrequest"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1).as("joincount"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING"===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1).as("successcount"),
//      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1).as("show"),
//      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1).as("click"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0).as("adpay"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0).as("adpprice")
//    )
//      .groupBy("devicetype")
//
//      .agg(count("original").as("original"),
//            count("effective").as("effective"),
//            count("adrequest").as("adrequest"),
//            count("joincount").as("joincount"),
//            count("successcount").as("successcount"),
//            (count("successcount") / count("joincount")).as("successrate"),
//
//            count("show").as("show"),
//            count("click").as("click"),
//            (count("click") / count("show")).as("clickrate"),
//
//            sum("adpay"),
//            sum("adpprice")).show()

//    ================================================================================
//    操作系统类

//    import org.apache.spark.sql.functions._
//    import sparkSession.implicits._
//    src.select(
//      when($"client"===1,"android")
//      when($"client"===2,"ios")
//      when($"client"===3,"wp")
//      when($"client"===1 and $"client"===2 and $"client"===3,"其他") as "client",
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1).as("original"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1).as( "effective"),
//      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1).as("adrequest"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1).as("joincount"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING"===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1).as("successcount"),
//      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1).as("show"),
//      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1).as("click"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0).as("adpay"),
//      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0).as("adpprice")
//    )
//      .groupBy("client")
//          .agg(count("original").as("original"),
//            count("effective").as("effective"),
//            count("adrequest").as("adrequest"),
//            count("joincount").as("joincount"),
//            count("successcount").as("successcount"),
//            (count("successcount") / count("joincount")).as("successrate"),
//
//            count("show").as("show"),
//            count("click").as("click"),
//            (count("click") / count("show")).as("clickrate"),
//
//            sum("adpay"),
//            sum("adpprice")).show()



  }
}
