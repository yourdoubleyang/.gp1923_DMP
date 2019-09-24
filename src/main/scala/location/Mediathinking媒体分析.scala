package location

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Mediathinking媒体分析 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    if (args.length != 1) {
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

    val df: DataFrame = sparkSession.read.text("in/app_dict.txt")
    import sparkSession.implicits._


    val ds: Dataset[(String, String)] = df.map(line => {
      val arr: Array[String] = line.toString().split("\\s", -1)
      arr
    }).filter(_.length >= 5).map(arr => (arr(4), arr(1)))




    val map: Map[String, String] = ds.collect().toMap

    val broadval: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)


    import org.apache.spark.sql.functions._
    src.select(
      when($"appname"=!="其他",$"appname").otherwise(broadval.value.getOrElse($"appid".toString(),"unknow")) as "appname",
      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 1, value = 1).as("original"),
      when($"REQUESTMODE" === 1 and $"PROCESSNODE" >= 2, value = 1).as( "effective"),
      when($"REQUESTMODE" === 1 and $"PROCESSNODE" === 3, value = 1).as("adrequest"),
      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1 and $"ISBID" === 1, value = 1).as("joincount"),
      when($"ISEFFECTIVE" === 1 and $"ISBILLING"===1 and $"ISWIN" === 1 and $"ADORDERID" =!= 0, 1).as("successcount"),
      when($"REQUESTMODE" === 2 and $"ISEFFECTIVE" === 1, value = 1).as("show"),
      when($"REQUESTMODE" === 3 and $"ISEFFECTIVE" === 1, value = 1).as("click"),
      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"adpayment" / 1000.0).as("adpay"),
      when($"ISEFFECTIVE" === 1 and $"ISBILLING" === 1, value = $"WinPrice" / 1000.0).as("adpprice")
    )
      .groupBy("appname")
      .agg(count("original").as("original"),
        count("effective").as("effective"),
        count("adrequest").as("adrequest"),
        count("joincount").as("joincount"),
        count("successcount").as("successcount"),
        (count("successcount") / count("joincount")).as("successrate"),

        count("show").as("show"),
        count("click").as("click"),
        (count("click") / count("show")).as("clickrate"),

        sum("adpay"),
        sum("adpprice")).show()

  }
}
