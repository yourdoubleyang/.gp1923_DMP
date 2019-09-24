package location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProCityCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath)=args

    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val src: DataFrame = sparkSession.read.parquet(inputPath)
    src.createTempView("log")
    val res: DataFrame = sparkSession.sql(
      """
select provincename,cityname,count(*) counts from log group by provincename,cityname
                         """.stripMargin
    )
    //将文件写入到磁盘，以json格式写入并且分区
    //res.write.partitionBy("provincename","cityname").json("D:\\spark项目\\项目day01\\Spark用户画像分析\\ProCityCountJson")

    //将文件写入到mysql
    //1,通过config配置文件依赖进行加载相关的配置信息
    val load: Config = ConfigFactory.load()

    //3.因为步骤2中需要传入一个properties对象，所以创建一个
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.put("password",load.getString("jdbc.password"))

    //2，存储
    res.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.table"),prop)

    sparkSession.stop()
  }
}
