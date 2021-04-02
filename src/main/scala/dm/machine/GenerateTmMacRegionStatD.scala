package dm.machine

import common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTmMacRegionStatD {
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
        .config("hive.metastore.uris",hiveMetastoreUris)
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
    sparkSession.sql(s"use $hiveDatabase ")
    sparkSession.sparkContext.setLogLevel("Error")

    sparkSession.sql(
      s"""
         |select
         |  PRVC,
         |  CTY,
         |  COUNT(MID) AS MAC_CNT,
         |  CAST(SUM(TOT_REV) AS DECIMAL(10,4)) AS MAC_REV,
         |  CAST(SUM(TOT_REF) AS DECIMAL(10,4)) AS MAC_REF,
         |  SUM(REV_ORDR_CNT) AS MAC_REV_ORDR_CNT,
         |  SUM(REF_ORDR_CNT) AS MAC_REF_ORDR_CNT,
         |  SUM(CNSM_USR_CNT) AS MAC_CNSM_USR_CNT,
         |  SUM(REF_USR_CNT) AS MAC_REF_USR_CNT
         |FROM TW_MAC_STAT_D
         |WHERE DATA_DT = ${analyticDate}
         |GROUP BY PRVC, CTY
         """.stripMargin
    ).createTempView("TEMP_MAC_REGION_STAT")


    sparkSession.sql(
      s"""
        | insert overwrite table TM_MAC_REGION_STAT_D partition(data_dt=$analyticDate) select * from TEMP_MAC_REGION_STAT
        """.stripMargin
    )

    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${analyticDate} as data_dt ,* from TEMP_MAC_REGION_STAT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_mac_region_stat_d",properties)

    println("**** all finished ****")
  }
}
