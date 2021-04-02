package eds.user

import common.ConfigUtils
import eds.machine.GenerateTwMacLoc.{hiveDataBase, hiveMetaStoreUris, localRun, sparkSession}
import org.apache.spark.sql.SparkSession

object GenerateTwCnsmBriefD {
  val localRun : Boolean = ConfigUtils.LOCAL_RUN
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBase = ConfigUtils.HIVE_DATABASE
  var sparkSession : SparkSession = _

  def main(args: Array[String]): Unit = {
    if(localRun){//本地运行
      sparkSession = SparkSession.builder().master("local")
        .config("hive.metastore.uris",hiveMetaStoreUris)
        .config("spark.sql.shuffle.partitions",10)
        .enableHiveSupport().getOrCreate()
    }else{//集群运行
      sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions",10).enableHiveSupport().getOrCreate()
    }

    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }
    val analyticDate = args(0)
    sparkSession.sql(s"use $hiveDataBase ")
    sparkSession.sparkContext.setLogLevel("Error")

    sparkSession.sql(
      """
        | select
        | ID,
        | TRD_ID,
        | cast(UID as string) AS UID,
        | MID,
        | PRDCD_TYPE,
        | PAY_TYPE,
        | ACT_TM,
        | PKG_ID,
        | case when AMT<0 then AMT*-1 else AMT end AS COIN_PRC,
        | 1 AS COIN_CNT,
        | ACT_TM as UPDATE_TM,
        | ORDR_ID,
        | ACTV_NM,
        | PKG_PRC,
        | PKG_DSCNT,
        | CPN_TYPE,
        | CASE WHEN ORDR_TYPE = 1 THEN 0
        |      WHEN ORDR_TYPE = 2 THEN 1
        |      WHEN ORDR_TYPE = 3 THEN 2
        |	      WHEN ORDR_TYPE = 4 THEN 2 END AS ABN_TYP
        |FROM TO_YCAK_CNSM_D
        """.stripMargin
    ).createTempView("TEMP_RESULT")

    sparkSession.sql(
      s"""
         | insert overwrite table TW_CNSM_BRIEF_D partition (data_dt=${analyticDate}) select * from temp_result
         """.stripMargin
    )

    println("**** all finished ****")

  }

}
