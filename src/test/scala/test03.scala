import com.alibaba.fastjson.{JSON, JSONObject}
import scalaj.http.{Http, HttpResponse}

object test03 {
  def main(args: Array[String]): Unit = {

    val response: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo?parameters")
      .param("key", "175a95ece450aab5a5433f45a5aa948d")
      .param("location", "120.1049,30.3197")
      .asString

    val nObject: JSONObject = JSON.parseObject(response.body.toString)
    println(s"nObject = " + nObject)
    println(s"infocode = ${nObject.getString("infocode")}")
    println(s"response.body = ${response.body}")
    println(s"response.code = ${response.code}")
    println(s"response.headers = ${response.headers}")
    println(s"response.cookies = ${response.cookies}")
  }

}
