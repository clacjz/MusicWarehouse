package eds.content

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONArray

object GenerateTwSongBaseInfoD {
  val localRun: Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetastoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDatabase = ConfigUtils.HIVE_DATABASE
  var sparkSession: SparkSession = _

  val getAlbumName: String => String = (albumInfo: String) => {
    var albumName = ""
    try {
      val array: fastjson.JSONArray = JSON.parseArray(albumName)
      albumName = array.getJSONObject(0).getString("name")
    } catch {
      case e: Exception => {
        if (albumInfo.contains("《") && albumInfo.contains("》")) {
          albumName = albumInfo.substring(albumInfo.indexOf("《") + 1, albumInfo.indexOf("》"))
        } else {
          albumName = "暂无专辑"
        }
      }
    }
    albumName
  }

  val getReleaseTime: String => String = (postTime: String) => {
    DateUtils.formatDate(postTime)
  }

  val getSingerInfo: (String, String, String) => String = (
                                                            singerInfo: String, singer: String, nameOrId: String
                                                          ) => {
    var singerNameOrSingerId = ""
    try {
      val jsonArray: fastjson.JSONArray = JSON.parseArray(singerInfo)
      if ("singer1".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("name")
      } else if ("singer1".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 0) {
        singerNameOrSingerId = jsonArray.getJSONObject(0).getString("id")
      } else if ("singer2".equals(singer) && "name".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("name")
      } else if ("singer2".equals(singer) && "id".equals(nameOrId) && jsonArray.size() > 1) {
        singerNameOrSingerId = jsonArray.getJSONObject(1).getString("id")
      }
    } catch {
      case e: Exception => {
        singerNameOrSingerId
      }
    }
    singerNameOrSingerId
  }

  val getAuthCompany: String => String = (authCompanyInfo: String) => {
    var authCompanyName = "乐心曲库"
    try {
      val jSONObject: JSONObject = JSON.parseObject(authCompanyInfo)
      authCompanyName = jSONObject.getString("name")
    } catch {
      case e: Exception => {
        authCompanyName
      }
    }
    authCompanyName
  }

  val getProductType : (String =>ListBuffer[Int]) = (productTypeInfo :String) => {
    val list = new ListBuffer[Int]()
    if(!"".equals(productTypeInfo.trim)){
      val strings = productTypeInfo.stripPrefix("[").stripSuffix("]").split(",")
      strings.foreach(t=>{
        list.append(t.toDouble.toInt)
      })
    }
    list
  }

  def main(args: Array[String]): Unit = {
    if (localRun) {
      sparkSession = SparkSession.builder()
        .master("local")
        .config("hive.metastore.uris", hiveMetastoreUris)
        .enableHiveSupport().getOrCreate()
    } else {
      sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    }

    import org.apache.spark.sql.functions._

    val udfGetAlbumName: UserDefinedFunction = udf(getAlbumName)
    val udfGetReleaseTime : UserDefinedFunction = udf(getReleaseTime)
    val udfGetSingerInfo : UserDefinedFunction = udf(getSingerInfo)
    val udfGetAuthCompany : UserDefinedFunction = udf(getAuthCompany)
    val udfGetProductType : UserDefinedFunction = udf(getProductType)

    sparkSession.sql(s"use $hiveDatabase")
    sparkSession.table("TO_SONG_INFO_D")
      .withColumn("ALBUM", udfGetAlbumName(col("ALBUM")))
      .withColumn("POST_TIME", udfGetReleaseTime(col("POST_TIME")))
      .withColumn("SINGER1", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("name")))
      .withColumn("SINGER1ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer1"), lit("id")))
      .withColumn("SINGER2", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("name")))
      .withColumn("SINGER2ID", udfGetSingerInfo(col("SINGER_INFO"), lit("singer2"), lit("id")))
      .withColumn("AUTH_CO", udfGetAuthCompany(col("AUTH_CO")))
      .withColumn("PRDCT_TYPE", udfGetProductType(col("PRDCT_TYPE")))
      .createTempView("TEMP_TO_SONG_INFO_D")

    sparkSession.sql(
      """
        | select NBR,
        |       nvl(NAME,OTHER_NAME) as NAME,
        |       SOURCE,
        |       ALBUM,
        |       PRDCT,
        |       LANG,
        |       VIDEO_FORMAT,
        |       DUR,
        |       SINGER1,
        |       SINGER2,
        |       SINGER1ID,
        |       SINGER2ID,
        |       0 as MAC_TIME,
        |       POST_TIME,
        |       PINYIN_FST,
        |       PINYIN,
        |       SING_TYPE,
        |       ORI_SINGER,
        |       LYRICIST,
        |       COMPOSER,
        |       BPM_VAL,
        |       STAR_LEVEL,
        |       VIDEO_QLTY,
        |       VIDEO_MK,
        |       VIDEO_FTUR,
        |       LYRIC_FTUR,
        |       IMG_QLTY,
        |       SUBTITLES_TYPE,
        |       AUDIO_FMT,
        |       ORI_SOUND_QLTY,
        |       ORI_TRK,
        |       ORI_TRK_VOL,
        |       ACC_VER,
        |       ACC_QLTY,
        |       ACC_TRK_VOL,
        |       ACC_TRK,
        |       WIDTH,
        |       HEIGHT,
        |       VIDEO_RSVL,
        |       SONG_VER,
        |       AUTH_CO,
        |       STATE,
        |       case when size(PRDCT_TYPE) =0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
        |    from TEMP_TO_SONG_INFO_D
        |    where NBR != ''
      """.stripMargin
    )
      .write.format("Hive")
      .mode(SaveMode.Overwrite)
      .saveAsTable("TW_SONG_BASEINFO_D")

    println("**** all finished ****")

  }


}
