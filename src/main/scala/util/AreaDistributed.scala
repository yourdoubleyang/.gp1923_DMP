package util

object AreaDistributed {
  //处理请求数
  def reqPt(requestmode:Int,processnode:Int)= {
    if (requestmode == 1 && processnode == 1) {
      List[Double](1, 0, 0)
    } else if (requestmode == 1 && processnode == 2) {
      List[Double](1, 1, 0)
    } else if (requestmode == 1 && processnode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

    //处理点击展示数
    def clickPt(requestmode:Int,iseffective:Int):List[Double]={
      if(requestmode==2 && iseffective==1){
        List[Double](1,0)
      }else if(requestmode==3 && iseffective ==1){
        List[Double](0,1)
      }else{
        List[Double](0,0)
      }
    }

    //处理竞价，成功数广告成品和消费
    def adPt(iseffective:Int,isbilling:Int,
             isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
      if(iseffective==1 && isbilling ==1 && isbid ==1){
        List(1,0,0,0,0)
      }else if(iseffective==1 && isbilling==1 && iswin==1 && adorderid != 0){
        List(0,1,0,0,0)
      }else if(iseffective==1 && isbilling==1 && iswin==1) {
        List(0, 1, 0, winprice / 1000, adpayment/1000)
      }else{
        List(0,0,0,0,0)
      }
    }

    //处理networkmannername的数据
    def getnetwork(ispname:String)={
      if(ispname=="电信"){
        "电信"
      }else if(ispname=="联通"){
        "联通"
      }else if(ispname=="移动"){
        "移动"
      }else{
        "其他"
      }
    }



}
