package eds.machine

import common.ConfigUtils
import eds.user.GenerateTwCnsmBriefD.{hiveDataBase, hiveMetaStoreUris, localRun, sparkSession}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTwMacStatD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if(localRun){//本地运行
      sparkSession = SparkSession.builder()
        .master("local")
        .config("hive.metastore.uris",hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions",10)
        .enableHiveSupport().getOrCreate()
    }else{//集群运行
      sparkSession = SparkSession.builder()
        .config("spark.sql.shuffle.partitions",10).enableHiveSupport().getOrCreate()
    }

    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }
    val analyticDate = args(0)
    sparkSession.sql(s"use $hiveDataBase ")
    sparkSession.sparkContext.setLogLevel("Error")

    sparkSession.table("TW_MAC_BASEINFO_D")
      .where(s"data_dt=${analyticDate}")
      .createTempView("TW_MAC_BASEINFO_D")

    sparkSession.table("TW_MAC_LOC_D")
      .where(s"data_dt=${analyticDate}")
      .createTempView("TW_MAC_LOC_D")

    sparkSession.table("TW_CNSM_BRIEF_D")
      .where(s"data_dt=${analyticDate}")
      .createTempView("TW_CNSM_BRIEF_D")

    sparkSession.table("TW_USR_BASEINFO_D")
      .where(s"data_dt=${analyticDate}")
      .createTempView("TW_USR_BASEINFO_D")

    sparkSession.sql(
      """
         |  select
         |    MID,
         |    PKG_ID,
         |    PAY_TYPE,
         |    COUNT(DISTINCT UID) AS CNSM_USR_CNT,
         |    SUM(COIN_CNT * COIN_PRC) AS TOT_REV,
         |    COUNT(ORDR_ID) AS REV_ORDR_CNT
         |  from TW_CNSM_BRIEF_D
         |  where ABN_TYP = 0
         |  group by MID, PKG_ID, PAY_TYPE
         """.stripMargin
    ).createTempView("TEMP_REV")


    sparkSession.sql(
      """
        | select
        |   MID,
        |   PKG_ID,
        |   PAY_TYPE,
        |   COUNT(DISTINCT UID) as REF_USR_CNT,
        |   SUM(COIN_CNT * COIN_PRC) as TOT_REF,
        |   COUNT(ORDR_ID) as REF_ORDR_CNT
        | from TW_CNSM_BRIEF_D
        | where ABN_TYP = 2
        | group by MID,PKG_ID,PAY_TYPE
        """.stripMargin
    ).createTempView("TEMP_REF")


    sparkSession.sql(
      s"""
        | select
        |   REG_MID as MID,
        |   count(UID) as NEW_USR_CNT
        | from TW_USR_BASEINFO_D
        | where REG_DT = ${analyticDate}
        | group by REG_MID
        """.stripMargin
    ).createTempView("TEMP_USR_NEW")


    sparkSession.sql(
      """
        | select
        |   A.MID,
        |   A.MAC_NM,
        |   A.PRDCT_TYP,
        |   A.STORE_NM,
        |   A.BUS_MODE,
        |   A.PAY_SW,
        |   A.SCENCE_CATGY,
        |   A.SUB_SCENCE_CATGY,
        |   A.SCENE,
        |   A.SUB_SCENE,
        |   A.BRND,
        |   A.SUB_BRND,
        |   NVL(B.PRVC, A.PRVC) AS PRVC,
        |   NVL(B.CTY, A.CTY) AS CTY,
        |   NVL(B.DISTRICT, A.AREA) AS AREA,
        |   A.PRTN_NM AS AGE_ID,
        |   A.INV_RATE,
        |   A.AGE_RATE,
        |   A.COM_RATE,
        |   A.PAR_RATE,
        |   C.PKG_ID,
        |   C.PAY_TYPE,
        |   NVL(C.CNSM_USR_CNT, 0) AS CNSM_USR_CNT,
        |   NVL(D.REF_USR_CNT, 0) AS REF_USR_CNT,
        |   NVL(E.NEW_USR_CNT, 0) AS NEW_USR_CNT,
        |   NVL(C.REV_ORDR_CNT, 0) AS REV_ORDR_CNT,
        |   NVL(D.REF_ORDR_CNT, 0) AS REF_ORDR_CNT,
        |   NVL(C.TOT_REV,0) AS TOT_REV,
        |   NVL(D.TOT_REF, 0) AS TOT_REF
        | from TW_MAC_BASEINFO_D A
        | left join TW_MAC_LOC_D B on A.MID = B.MID
        | left join TEMP_REV C on A.MID = C.MID
        | left join TEMP_REF D on A.MID = D.MID
        |           AND C.MID = D.MID
        |           AND C.PKG_ID = D.PKG_ID
        |           AND C.PAY_TYPE = D.PAY_TYPE
        | left join TEMP_USR_NEW E on A.MID = E.MID
        """.stripMargin
    ).createTempView("TEMP_MAC_RESULT")

    sparkSession.sql(
      s"""
         | insert overwrite table TW_MAC_STAT_D partition (data_dt = ${analyticDate}) select * from TEMP_MAC_RESULT
      """.stripMargin)

    /**
     * 同时将以上结果保存至 mysql songresult 库中的 machine_infos 中,作为DM层结果
     */
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from TEMP_MAC_RESULT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_machine_rev_infos",properties)

    println("**** all finished ****")
  }

}
