package eds.machine

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import common.{ConfigUtils, DateUtils, StringUtils}
import javafx.scene.chart.PieChart.Data
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scalaj.http.{Http, HttpOptions, HttpResponse}

import scala.collection.mutable.ListBuffer

object GenerateTwMacLocD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  def getLocInfoFromGaodeAPI(rowList: List[Row]): ListBuffer[Row] = {
    val returnLocList = new ListBuffer[Row]()
    var concatYX = ""
    for (i <- 0 until rowList.size) {
      val X: String = rowList(i).getAs[String]("X")
      val Y: String = rowList(i).getAs[String]("Y")
      concatYX += Y + "," + X + "|"
    }

    val response: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo?parameters")
      .param("key", "175a95ece450aab5a5433f45a5aa948d")
      .param("location",concatYX.substring(0, concatYX.length-1))
      .param("batch", "true")
      .option(HttpOptions.readTimeout(10000))
      .asString

    val jsonInfo: JSONObject = JSON.parseObject(response.body.toString)
    val returnLocLength: Int = JSON.parseArray(jsonInfo.getString("regeocodes")).size()

    if ("10000".equals(jsonInfo.getShort("infocode")) && rowList.size == returnLocLength) {

      val jSONArray: JSONArray = JSON.parseArray(jsonInfo.getString("regeocodes"))
      for (i <- 0 until rowList.length) {
        val mid: Int = rowList(i).getAs[String]("MID").toInt
        val x: String = rowList(i).getAs[String]("X")
        val y: String = rowList(i).getAs[String]("Y")
        val cnt: Int = rowList(i).getAs[String]("CNT").toInt
        val currentJsonObject = jSONArray.getJSONObject(i)

        val address = StringUtils.checkString(currentJsonObject.getString("formatted_address"))
        val province = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("province"))
        val city = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("city"))
        val cityCode = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("citycode"))
        val district = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("district"))
        val adCode = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("adcode"))
        val townShip = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("township"))
        val townCode = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("towncode"))
        val neighborhoodName = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("neighborhood").getString("name"))
        val neighborhoodType = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("neighborhood").getString("type"))
        val buildingName = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("building").getString("name"))
        val buildingType = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("building").getString("type"))
        val street = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("streetNumber").getString("street"))
        val number = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("streetNumber").getString("number"))
        val location = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("streetNumber").getString("location"))
        val direction = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("streetNumber").getString("direction"))
        val distance = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getJSONObject("streetNumber").getString("distance"))
        val businessAreas = StringUtils.checkString(currentJsonObject.getJSONObject("addressComponent").getString("businessAreas"))
        returnLocList.append(Row(mid, x, y, cnt, address, province, city, cityCode, district, adCode, townShip, townCode,
          neighborhoodName, neighborhoodType, buildingName, buildingType, street, number, location, direction, distance, businessAreas))
      }
    }
    returnLocList
  }

  def main(args: Array[String]): Unit = {
    if (localRun) { //本地运行
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris", hiveMetastoreUris)
        .config("spark.sql.shuffle.partitions", 10)
        .enableHiveSupport().getOrCreate()
    } else { //集群运行
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions", 10).enableHiveSupport().getOrCreate()
    }

    if (args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }
    val analyticDate = args(0)
    sparkSession.sql(s"use ${hiveDatabase} ")
    sparkSession.sparkContext.setLogLevel("Error")

    val pre30Date: String = DateUtils.getCurrentDatePreDate(analyticDate, 700)

    val pre30DaysDataFrame: DataFrame = sparkSession.sql(
      s"""
         | select
         |  UID,
         |  MID,
         |  LAT,
         |  LNG
         | from TO_YCAK_USR_LOC_D
         | where data_dt between ${pre30Date} and ${analyticDate}
         """.stripMargin
    )

    pre30DaysDataFrame
      .distinct()
      .groupBy("MID","LAT", "LNG")
      .count()
      .withColumnRenamed("LAT", "X")
      .withColumnRenamed("LNG", "Y")
      .withColumnRenamed("count", "CNT")
      .createTempView("TEMP_PRE30_MAC_LOC_INFO")

    val macLocDF : DataFrame = sparkSession.sql(
      """
        |select
        | MID, --机器
        | X,   --纬度
        | Y,   --经度
        | CNT, --出现次数
        | row_number() over(partition by MID order by CNT desc) as RANK
        |from TEMP_PRE30_MAC_LOC_INFO
      """.stripMargin).filter("x != '' and y != '' and RANK = 1")

    val rowRDD: RDD[Row] = macLocDF.rdd.mapPartitions(
      iter => {
        val detailLocalInfo = new ListBuffer[Row]()
        val list: List[Row] = iter.toList
        val length: Int = list.size
        var times = 0

        times = (length + 99) / 100

        for (i <- 0 until times) {
          val currentRowList: List[Row] = list.slice(i * 100, i * 100 + 100)
          val rows: ListBuffer[Row] = getLocInfoFromGaodeAPI(currentRowList)
          detailLocalInfo.++=(rows)
        }
        detailLocalInfo.iterator
      }
    )

    val schema = StructType(Array[StructField](
      StructField("MID", IntegerType),
      StructField("X", StringType),
      StructField("Y", StringType),
      StructField("CNT", IntegerType ),
      StructField("ADDR", StringType ),
      StructField("PRVC", StringType ),
      StructField("CTY", StringType ),
      StructField("CTY_CD", StringType ),
      StructField("DISTRICT", StringType ),
      StructField("AD_CD", StringType ),
      StructField("TOWN_SHIP", StringType ),
      StructField("TOWN_CD", StringType ),
      StructField("NB_NM", StringType ),
      StructField("NB_TP", StringType ),
      StructField("BD_NM", StringType ),
      StructField("BD_TP", StringType ),
      StructField("STREET", StringType ),
      StructField("STREET_NB", StringType ),
      StructField("STREET_LOC", StringType ),
      StructField("STREET_DRCTION", StringType ),
      StructField("STREET_DSTANCE", StringType ),
      StructField("BUS_INFO", StringType )
    ))

    import org.apache.spark.sql.functions._
    val pre30DatsMacLocInfo: DataFrame = sparkSession.createDataFrame(rowRDD, schema)

    val preDate: String = DateUtils.getCurrentDatePreDate(analyticDate, 1)
    val preDataMacLocInfo: Dataset[Row] = sparkSession.table("TW_MAC_LOC_D")
      .where(s"data_dt = ${preDate}")

    val diffMid: Dataset[Row] = preDataMacLocInfo.select("MID")
      .except(pre30DatsMacLocInfo.select("MID"))
    // 按照mid 左连接 关联per1DateMacLocInfo 获取30天前的机器详细信息然后与当前计算的 最近30天机器信息做union
    val allMacLocInfos = diffMid.join(preDataMacLocInfo,Seq("mid"),"left")
      .drop(col("data_dt")).union(pre30DatsMacLocInfo)
    allMacLocInfos.createTempView("TEMP_ALL_MAC_LOC_INFO")

    sparkSession.sql(
      s"""
        |insert overwrite table tw_mac_loc_d partition(data_dt=${analyticDate}) select * from TEMP_ALL_MAC_LOC_INFO
      """.stripMargin)

    println("**** all finished ****")

  }
}
