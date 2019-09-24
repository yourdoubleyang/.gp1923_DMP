package location

import org.apache.spark.sql.{Dataset, Row}

object testaboutzip {
  def main(args: Array[String]): Unit = {
    val list1:List[Int] = List(1,1,1,1,1,1)
    val list2:List[Int] = List(2,2,2,2,2,2)
    val tuples: List[(Int, Int)] = list1.zip(list2)
    println(tuples)


    val a = (List(1,1,1,1,1),"abc")
    val b = (List(2,2,2,2,2),"eee")



  }

}
