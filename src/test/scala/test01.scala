import base.PairRDDMultipleTextOutputFormat
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object test01 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val conf: SparkConf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")

    val context = new SparkContext(conf)
    context.setLogLevel("ERROR")

    val lines: RDD[String] = context.textFile("file:///C:/Users/clacjz/IdeaProjects/MusicProject/src/main/resources/clientlog/*")

   lines.map(line => {line.split("&")})
      //.filter(item => item.size == 6)
      .map(line => (line(2), line(3)))
      .map(tp => {
        val cmd: String = tp._1
        val jsonStr: String = tp._2
        if (cmd.equals("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")) {
          val jsonObj: JSONObject = JSON.parseObject(jsonStr)
          val songid: String = jsonObj.getString("songid")
          val mid: String = jsonObj.getString("mid")
          val oprateType: String = jsonObj.getString("optrate_type")
          val uid: String = jsonObj.getString("uid")
          val consumeType: String = jsonObj.getString("consume_type")
          val playTime: String = jsonObj.getString("play_time")
          val durTime: String = jsonObj.getString("dur_time")
          val sessionID: String = jsonObj.getString("session_id")
          val songName: String = jsonObj.getString("songname")
          val pkgID: String = jsonObj.getString("pkg_id")
          val orderID: String = jsonObj.getString("order_id")
          (cmd, s"$songid\t$mid\t$oprateType\t$uid\t$consumeType\t$playTime\t$durTime\t$sessionID\t$songName\t$pkgID\t$orderID")
        } else {
          tp
        }
      }).saveAsHadoopFile(
     "hdfs://mycluster/testdata",
     classOf[String],
     classOf[String],
     classOf[PairRDDMultipleTextOutputFormat])
  }

}
