package ods

import base.PairRDDMultipleTextOutputFormat
import com.alibaba.fastjson.{JSON, JSONObject}
import common.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ProduceClientLog {
  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDataBase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession : SparkSession = _
  private var sparkContext : SparkContext = _
  private val hdfsClientLogPath: String = ConfigUtils.HDFS_CLIENT_LOG_PATH
  private var clientLogInfo : RDD[String] = _

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      print(s"需要指定数据日期")
      System.exit(1)
    }

    val logDate = args(0)

    if (localrun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ProduceClientLog")
        .config("spark.hadoop.ignoreInputErrors","true")
        .config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      clientLogInfo = sparkContext.textFile("file:///I:/minik_jo_20191203")
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      clientLogInfo = sparkContext.textFile(s"${hdfsClientLogPath}/currentday_clientlog.tar.gz")
    }
    val tableNameAndInfo: RDD[(String, String)] = clientLogInfo.map(line => {line.split("&")})
      .filter(item => item.length == 6)
      .map(line => (line(2), line(3)))

    tableNameAndInfo.map(tp => {
      var tableName = tp._1
      var info = tp._2
      if (tableName.equals("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")) {
        val jsonObject: JSONObject = JSON.parseObject(info)
        val songId: String = jsonObject.getString("songid")
        val operateType: String = jsonObject.getString("optrate_type")
        val uid: String = jsonObject.getString("uid")
        val consumeType: String = jsonObject.getString("consume_type")
        val playTime: String = jsonObject.getString("play_time")
        val durTime: String = jsonObject.getString("dur_time")
        val sessionID: String = jsonObject.getString("session_id")
        val songName: String = jsonObject.getString("songname")
        val pkgID: String = jsonObject.getString("pkg_id")
        val orderID: String = jsonObject.getString("order_id")
        (tableName, songId + "\t" + operateType + "\t" + uid + "\t"
          + consumeType + "\t" + playTime + "\t" + durTime + "\t"
          + sessionID + "\t" + songName + "\t" + pkgID + "\t" + orderID)
      } else {
        tp
      }
    }).saveAsHadoopFile(
      s"${hdfsClientLogPath}/all_client_tables/${logDate}",
      classOf[String],
      classOf[String],
      classOf[PairRDDMultipleTextOutputFormat]
    )

    sparkSession.sql(s"use $hiveDataBase")
    sparkSession.sql(
      """
        | CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D` (
        | `SONGID` string,  --歌曲ID
        | `MID` string,     --机器ID
        | `OPTRATE_TYPE` string,  --操作类型
        | `UID` string,     --用户ID
        | `CONSUME_TYPE` string,  --消费类型
        | `DUR_TIME` string,      --时长
        | `SESSION_ID` string,    --sessionID
        | `SONGNAME` string,      --歌曲名称
        | `PKG_ID` string,        --套餐ID
        | `ORDER_ID` string       --订单ID
        | )
        | partitioned by (data_dt string)
        | ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        | LOCATION 'hdfs://mycluster/user/hive/warehouse/data/song/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D'
        |""".stripMargin
    )

    sparkSession.sql(
      s"""
         | load data inpath
         | '${hdfsClientLogPath}/all_client_tables/${logDate}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ'
         | into table TO_CLIENT_SONG_PLAY_OPERATE_REQ_D partition (data_dt='${logDate}')
      """.stripMargin)

    println("**** all finished ****")
  }

}
