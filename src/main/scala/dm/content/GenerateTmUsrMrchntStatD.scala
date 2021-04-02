package dm.content

import common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTmUsrMrchntStatD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if (localRun) { //本地运行
      sparkSession = SparkSession.builder()
        .master("local")
        .config("hive.metastore.uris", hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions", 10)
        .enableHiveSupport().getOrCreate()
    } else { //集群运行
      sparkSession = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", 10).enableHiveSupport().getOrCreate()
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
         |  AGE_ID AS ADMIN_ID,
         |  PAY_TYPE,
         |  SUM(REV_ORDR_CNT) AS REV_ORDR_CNT,
         |  SUM(REF_ORDR_CNT) AS REF_ORDR_CNT,
         |  CAST(SUM(TOT_REV) AS Double) AS TOT_REV,
         |  CAST(SUM(TOT_REF) AS Double) AS TOT_REF,
         |  CAST(SUM(TOT_REF * NVL(INV_RATE, 0)) AS DECIMAL(10,4)) AS TOT_INV_REV,
         |  CAST(SUM(TOT_REF * NVL(AGE_RATE, 0)) AS DECIMAL(10,4)) AS TOT_AGE_REV,
         |  CAST(SUM(TOT_REF * NVL(COM_RATE, 0)) AS DECIMAL(10,4)) AS TOT_COM_REV,
         |  CAST(SUM(TOT_REF * NVL(PAR_RATE, 0)) AS DECIMAL(10,4)) AS TOT_PAR_REV
         | from TW_MAC_STAT_D
         | WHERE DATA_DT = ${analyticDate}
         | GROUP BY AGE_ID, PAY_TYPE
         """.stripMargin
    ).createTempView("TEMP_USR_MRCHNT_STAT")

    sparkSession.sql(
      s"""
        | insert overwrite table TM_USR_MRCHNT_STAT_D partition(data_dt = ${analyticDate}) select * from TEMP_USR_MRCHNT_STAT
        """.stripMargin
    )

    val properties = new Properties()
    properties.setProperty("user", mysqlUser)
    properties.setProperty("password", mysqlPassword)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    sparkSession.sql(
      s"""
        | select $analyticDate as data_dt ,* from TEMP_USR_MRCHNT_STAT
        """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_usr_mrchnt_stat_d",properties)

  }
}
