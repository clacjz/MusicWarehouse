package eds.user

import common.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTwUserBaseinfoD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }

    if (localRun) {
      sparkSession = SparkSession.builder().master("local").appName("Generate_TM_User_Baseinfo_D")
        .config("spark.sql.shuffle.partitions", "1")
        .config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
      sparkSession.sparkContext.setLogLevel("Error")
    } else {
      sparkSession = SparkSession.builder().appName("Generate_TM_User_Baseinfo_D").enableHiveSupport().getOrCreate()
    }

    val currentDate: String = args(0)

    sparkSession.sql(s"use ${hiveDatabase}")

    val usrVx = sparkSession.sql(
      """
        | select
        |   uid,
        |   reg_mid,
        |   "1" as reg_chnl,
        |   wx_id as ref_uid,
        |   gdr,
        |   birthday,
        |   msisdn,
        |   loc_id,
        |   log_mde,
        |   substring(reg_tm, 1, 8) as reg_dt,
        |   substring(reg_tm, 9, 6) as reg_tm,
        |   usr_exp,
        |   score,
        |   level,
        |   "2" as usr_type,
        |   NULL as is_cert,
        |   NULL as is_stdnt
        | from TO_YCAK_USR_D
        """.stripMargin
    )

    val usrAli = sparkSession.sql(
      """
        | select
        |   uid,
        |   reg_mid,
        |   "2" as reg_chnl,
        |   aly_id as ref_uid,
        |   gdr,
        |   birthday,
        |   msisdn,
        |   loc_id,
        |   log_mde,
        |   substring(reg_tm, 1, 8) as reg_dt,
        |   substring(reg_tm, 9, 6) as reg_tm,
        |   usr_exp,
        |   score,
        |   level,
        |   nvl(usr_type, "2") as usr_type,
        |   NULL as is_cert,
        |   NULL as is_stdnt
        | from TO_YCAK_USR_ALI_D
        """.stripMargin
    )

    val usrQQ = sparkSession.sql(
      """
        | select
        |   uid,
        |   reg_mid,
        |   "3" as reg_chnl,
        |   QQID as ref_uid,
        |   gdr,
        |   birthday,
        |   msisdn,
        |   loc_id,
        |   log_mde,
        |   substring(reg_tm, 1, 8) as reg_dt,
        |   substring(reg_tm, 9, 6) as reg_tm,
        |   usr_exp,
        |   score,
        |   level,
        |   "2" as usr_type,
        |   NULL as is_cert,
        |   NULL as is_stdnt
        | from TO_YCAK_USR_QQ_D
        """.stripMargin
    )

    val usrAPP = sparkSession.sql(
      """
        | select
        |   uid,
        |   reg_mid,
        |   "4" as reg_chnl,
        |   app_id as ref_uid,
        |   gdr,
        |   birthday,
        |   msisdn,
        |   loc_id,
        |   NULL as log_mde,
        |   substring(reg_tm, 1, 8) as reg_dt,
        |   substring(reg_tm, 9, 6) as reg_tm,
        |   usr_exp,
        |   0 as score,
        |   level,
        |   "2" as usr_type,
        |   NULL as is_cert,
        |   NULL as is_stdnt
        | from TO_YCAK_USR_APP_D
        """.stripMargin
    )

    val allUserInfo = usrVx.union(usrAli).union(usrQQ).union(usrAPP)

    sparkSession.table("TO_YCAK_USR_LOGIN_D")
      .where(s"data_dt=${currentDate}")
      .select("UID")
      .distinct()
      .join(allUserInfo, Seq("UID"), "left")
      .createTempView("TEMP_USR_ACTV")

    sparkSession.sql(
      s"""
        | insert overwrite table TW_USR_BASEINFO_D partition (data_dt = $currentDate)
        | select * from TEMP_USR_ACTV
        """.stripMargin
    )

    val pre7Date: String = DateUtils.getCurrentDatePreDate(currentDate, 7)

    val properties = new Properties()
    properties.setProperty("user", mysqlUser)
    properties.setProperty("password", mysqlPassword)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

    sparkSession.sql(
      s"""
         | select
         |      A.UID,
         |      case when B.reg_chnl = '1' then '微信'
         |           when B.reg_chnl = '2' then '支付宝'
         |           when B.reg_chnl = '3' then 'QQ'
         |           when B.reg_chnl = '4' then 'APP'
         |           else '未知' end reg_chnl,
         |      B.REF_UID,
         |      case when B.GDR = '0' then '不明'
         |           when B.GDR = '1' then '男'
         |           when B.GDR = '2' then '女'
         |           else '' end gnr,
         |      B.birthday,
         |      B.msisdn,
         |      B.reg_dt,
         |      B.level
         | from
         |  (
         |    select
         |      uid, count(*) as c
         |    from TW_USR_BASEINFO_D
         |    where data_dt between ${pre7Date} and ${currentDate}
         |    group by UID having c = 1
         |   ) A,
         |   TW_USR_BASEINFO_D B
         |   where B.data_dt = ${currentDate} and A.UID = B.UID
         """.stripMargin
    ).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "user_7days_active", properties)

    println("**** all finished ****")
  }
}
