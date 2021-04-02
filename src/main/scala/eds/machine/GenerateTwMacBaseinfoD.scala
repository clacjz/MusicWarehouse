package eds.machine

import common.ConfigUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.Properties

object GenerateTwMacBaseinfoD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD

  val getFormatTime = ((time: String) => {
    var formatTime: String = time
    if (time.length == 14) {
      formatTime
    } else if (time.length < 14 && time.length == 8) {
      formatTime = time + "000000";
    } else {
      formatTime = "19700101000000"
    }
    formatTime
  })

  val getProvinceInfo = (province: String, addr: String) => {
    var prov = ""
    if (province == "null") {
      if (addr != null) {
        prov = getProvince(addr)
      }
    } else {
      prov = province;
    }
    prov
  }

  val getCityInfo = (cty: String, addr: String) => {
    var city = ""
    if (cty == "null") {
      if (addr != null) {
        city = getCity(addr)
      }
    }
    city
  }

  def getProvince(addr: String): String = {
    var returnProvince = ""
    if (addr.contains("内蒙古自治区")) {
      returnProvince = "内蒙古"
    } else if (addr.contains("宁夏回族自治区")) {
      returnProvince = "宁夏"
    } else if (addr.contains("西藏自治区")) {
      returnProvince = "西藏"
    } else if (addr.contains("广西壮族自治区")) {
      returnProvince = "广西"
    } else if (addr.contains("新疆维吾尔自治区")) {
      returnProvince = "新疆"
    } else if (addr.contains("北京市")) {
      returnProvince = "北京"
    } else if (addr.contains("上海市")) {
      returnProvince = "上海"
    } else if (addr.contains("重庆市")) {
      returnProvince = "重庆"
    } else if (addr.contains("天津市")) {
      returnProvince = "天津"
    } else if (addr.contains("省")) {
      returnProvince = addr.substring(0, addr.indexOf("省"))
    }
    returnProvince
  }

  def getCity(addr: String) = {
    var returnCity = ""
    if (addr.contains("内蒙古自治区")) {
      returnCity = addr.substring(addr.indexOf("区") + 1, addr.indexOf("市"))
    } else if (addr.contains("宁夏回族自治区")) {
      returnCity = addr.substring(addr.indexOf("区") + 1, addr.indexOf("市"))
    } else if (addr.contains("广西壮族自治区")) {
      returnCity = addr.substring(addr.indexOf("区") + 1, addr.indexOf("市"))
    } else if (addr.contains("新疆维吾尔自治区")) {
      try {
        returnCity = addr.substring(addr.indexOf("区") + 1, addr.indexOf("市"))
      } catch {
        case e: Exception => {
          val index = addr.indexOf("区")
          if (addr.substring(index + 1, addr.length).contains("区")) {
            returnCity = addr.substring(index + 1, addr.indexOf("区", index + 1))
          } else {
            returnCity = addr.substring(index + 1, addr.indexOf("州", index + 1))
          }
        }
      }
    } else if (addr.contains("北京市")) {
      returnCity = addr.substring(addr.indexOf("市") + 1, addr.indexOf("区"))
    } else if (addr.contains("上海市")) {
      returnCity = addr.substring(addr.indexOf("市") + 1, addr.indexOf("区"))
    } else if (addr.contains("重庆市")) {
      val index = addr.indexOf("市")
      if (addr.substring(index + 1, addr.length).contains("区")) {
        returnCity = addr.substring(addr.indexOf("市") + 1, addr.indexOf("区"))
      } else if (addr.substring(index + 1, addr.length).contains("县")) {
        returnCity = addr.substring(addr.indexOf("市") + 1, addr.indexOf("县"))
      }

    } else if (addr.contains("天津市")) {
      returnCity = addr.substring(addr.indexOf("市") + 1, addr.indexOf("区"))
    } else if (addr.contains("省")) {
      val index = addr.indexOf("省")
      if (addr.substring(index + 1, addr.length).contains("市")) {
        returnCity = addr.substring(index + 1, addr.indexOf("市"))
      } else if (addr.substring(index + 1, addr.length).contains("州")) {
        returnCity = addr.substring(index + 1, addr.substring(index + 1, addr.length).indexOf("州"))
      }
      returnCity
    }
    returnCity
  }

  def main(args: Array[String]): Unit = {
    if (localRun) {
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

    sparkSession.sql(s"use $hiveDatabase")

    import org.apache.spark.sql.functions._

    val udfGetFormatTime: UserDefinedFunction = udf(getFormatTime)
    val udfGetProvinceInfo: UserDefinedFunction = udf(getProvinceInfo)
    val udfGetCityInfo: UserDefinedFunction = udf(getCityInfo)

    val frame1: DataFrame = sparkSession.table("TO_YCAK_MAC_D")
      .withColumn("PRDCT_TYPE", lit(2))
    frame1.createTempView("TO_TCAK_MAC_D")

    val frame2: DataFrame = sparkSession.table("TO_YCBK_MAC_ADMIN_MAP_D")
      .withColumn("ACTV_TM", udfGetFormatTime(col("ACTV_TM")))
      .withColumn("ORDER_TM", udfGetFormatTime(col("ORDER_TM")))
    frame2.createTempView("TO_YCBK_MAC_ADMIN_MAP_D")

    frame1.select("MID", "PRDCT_TYPE")
      .union(frame2.select("MID", "PRDCT_TYPE"))
      .distinct().createTempView("TEMP_MAC_ALL")

    val provinceOrCityIsNotNullDF = sparkSession.table("TO_YCAK_MAC_LOC_D")
      .filter("prvc != 'null' and cty != 'null'")

    sparkSession.table("TO_YCAK_MAC_LOC_D")
      .filter("prvc = 'null' and cty = 'null'")
      .withColumn("PRVC",udfGetProvinceInfo(col("PRVC"),col("ADDR")))
      .withColumn("CTY",udfGetCityInfo(col("CTY"),col("ADDR")))
      .union(provinceOrCityIsNotNullDF)
      .filter("prvc != '' and cty != ''")
      .withColumn("REV_TM",udfGetFormatTime(col("REV_TM")))
      .withColumn("SALE_TM",udfGetFormatTime(col("SALE_TM")))
      .createTempView("TO_YCAK_MAC_LOC_D")


    /****************************************************************************************
     *  获取 机器上歌曲版本 机器位置信息
     * 以 TEMP_MAC_ALL 中的MID 为基准，对 TO_YCAK_MAC_D、TO_YCAK_MAC_LOC_D两张表的信息进行统计
     *
     */
    sparkSession.sql(
      """
        |SELECT
        |   TEMP.MID, --机器ID
        |   MAC.SRL_ID, --序列号
        |   MAC.HARD_ID,	--硬件ID
        |   MAC.SONG_WHSE_VER, --歌库版本号
        |   MAC.EXEC_VER,	--系统版本号
        |   MAC.UI_VER,		--歌库UI版本号
        |   MAC.STS, --激活状态
        |   MAC.CUR_LOGIN_TM,	--最近登录时间
        |   MAC.PAY_SW,   --支付开关是否打开
        |   MAC.IS_ONLINE, --是否在线
        |   TEMP.PRDCT_TYPE,
        |   LOC.PRVC,	--机器所在省份
        |   LOC.CTY,	--机器所在城市
        |   LOC.ADDR_FMT,	--详细地址
        |   LOC.REV_TM,	--运营时间
        |   LOC.SALE_TM 	--销售时间
        |from TEMP_MAC_ALL as TEMP
        |left join TO_YCAK_MAC_D as  MAC  on TEMP.MID = MAC.MID
        |left join TO_YCAK_MAC_LOC_D as LOC on TEMP.MID = LOC.MID
      """.stripMargin).createTempView("TEMP_YCAK_MAC_INFO")


    /** **********************************************************
     * 获取机器 套餐名称、投资分成、机器门店、场景信息
     * 以TEMP_MAC_ALL 表为基准， 对 ycbk 系统对应的ODS层数据 进行统计。
     */
    sparkSession.sql(
      """
        | select
        |    TEMP.MID,    --机器ID
        |    MA.MAC_NM, 		--机器名称
        |    MA.PKG_NM, 		--套餐名称
        |    MA.INV_RATE, 	--投资人分成比例
        |    MA.AGE_RATE,		--承接方分成比例
        |    MA.COM_RATE,		--公司分成比例
        |    MA.PAR_RATE,		--合作方分成比例
        |    MA.IS_ACTV, 		--是否激活
        |    MA.ACTV_TM,		--激活时间
        |    MA.HAD_MPAY_FUNC as PAY_SW,   --支付开关是否打开
        |    PRVC.PRVC, 		--省份
        |    CTY.CTY, 		--城市
        |    AREA.AREA,  		--区、县
        |    CONCAT(MA.SCENE_ADDR,MA.GROUND_NM) as ADDR, 	--场景地址,场地名称,
        |    STORE.GROUND_NM as STORE_NM,	--门店名称,这里的store_nm都是数字
        |    STORE.TAG_NM,	--主场景名称
        |    STORE.SUB_TAG_NM,--主场景分类
        |    STORE.SUB_SCENE_CATGY_NM,	--子场景分类名称
        |    STORE.SUB_SCENE_NM,	--子场景名称
        |    STORE.BRND_NM,		--品牌名称
        |    STORE.SUB_BRND_NM	--子品牌名称
        | from TEMP_MAC_ALL as TEMP
        | left join TO_YCBK_MAC_ADMIN_MAP_D as MA on TEMP.MID = MA.MID
        | left join TO_YCBK_PRVC_D as PRVC on MA.SCENE_PRVC_ID = PRVC.PRVC_ID
        | left join TO_YCBK_CITY_D as CTY on MA.SCENE_CTY_ID = CTY.CTY_ID
        | left join TO_YCBK_AREA_D as AREA on MA.SCENE_AREA_ID = AREA.AREA_ID
        | left join TO_YCBK_MAC_STORE_MAP_D as SMA on TEMP.MID = SMA.MID
        | left join TO_YCBK_STORE_D as STORE on SMA.STORE_ID =  STORE.ID
      """.stripMargin).createTempView("TEMP_YCBK_MAC_INFO")

    sparkSession.sql(
      s"""
         | select
         | 	YCAK.MID,                        --机器ID
         | 	YCBK.MAC_NM,                    --机器名称
         | 	YCAK.SONG_WHSE_VER,              --歌曲版本
         | 	YCAK.EXEC_VER,	                   --系统版本号
         | 	YCAK.UI_VER,		                   --歌曲UI版本号
         | 	YCAK.HARD_ID,                    --硬件ID
         | 	YCAK.SALE_TM,	                   --销售时间
         | 	YCAK.REV_TM,		                   --运营时间
         | 	YCBK.STORE_NM as OPER_NM,       --运营商名称
         | 	if (YCAK.PRVC is null,YCBK.PRVC,YCAK.PRVC) as PRVC,		--机器所在省
         | 	if (YCAK.CTY is null,YCBK.CTY,YCAK.CTY) as CTY,		    --机器所在市
         | 	YCBK.AREA,				                                  --机器所在区域
         | 	if (YCAK.ADDR_FMT is null,YCBK.ADDR,YCAK.ADDR_FMT) as ADDR,     --机器详细地址
         | 	YCBK.STORE_NM,	                                   --门店名称
         | 	YCBK.TAG_NM as SCENCE_CATGY,                      --主场景名称
         | 	YCBK.SUB_SCENE_CATGY_NM as SUB_SCENCE_CATGY,      --子场景分类名称
         | 	YCBK.SUB_TAG_NM as SCENE ,                        --主场景分类名称
         | 	YCBK.SUB_SCENE_NM as SUB_SCENE ,                  --子场景名称
         | 	YCBK.BRND_NM as BRND,	                            --主场景品牌
         | 	YCBK.SUB_BRND_NM as SUB_BRND, 	                  --子场景品牌
         | 	YCBK.PKG_NM as PRDCT_NM,                          --产品名称
         | 	2 as PRDCT_TYP,	                                  --产品类型
         | 	case when YCBK.PKG_NM = '联营版' then '联营'
         | 	     when YCBK.INV_RATE < 100 then '联营'
         | 	     else '卖断' end BUS_MODE,                     --运营模式
         | 	YCBK.INV_RATE, 	                                  --投资人分成比例
         | 	YCBK.AGE_RATE,	                                    --代理人、联盟人分成比例
         | 	YCBK.COM_RATE,	                                    --公司分成比例
         | 	YCBK.PAR_RATE,	                                    --合作方分成比例
         | 	if (YCAK.STS is null ,YCBK.IS_ACTV,YCAK.STS) as IS_ACTV,    	--是否激活
         | 	YCBK.ACTV_TM,	                --激活时间
         | 	if (YCAK.PAY_SW is null ,YCBK.PAY_SW,YCAK.PAY_SW) as PAY_SW,  --是否开通移动支付
         | 	YCBK.STORE_NM as PRTN_NM,	   --代理人姓名，这里获取门店名称
         | 	YCAK.CUR_LOGIN_TM	          --最近登录时间
         | FROM TEMP_YCAK_MAC_INFO as YCAK
         | LEFT JOIN  TEMP_YCBK_MAC_INFO as YCBK
         | ON YCAK.MID = YCBK.MID
         |
      """.stripMargin).createTempView("result")

    sparkSession.sql(s"use $hiveDatabase")
    //将以上结果保存到 Hive EDS层分区表
    sparkSession.sql(
      s"""
         | insert overwrite table tw_mac_baseinfo_d partition(data_dt=${analyticDate}) select * from result
        """.stripMargin)

    //将以上结果写入到MySql中提供查询
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from result
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_mac_baseinfo",properties)

    println("**** all finished ****")


  }

}
