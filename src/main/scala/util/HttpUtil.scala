package util


import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * http请求
  */
object HttpUtil {
  //get请求
  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpget = new HttpGet(url)

    //发送请求
    val response: CloseableHttpResponse = client.execute(httpget)
    EntityUtils.toString(response.getEntity,"UTF-8")

  }
}
