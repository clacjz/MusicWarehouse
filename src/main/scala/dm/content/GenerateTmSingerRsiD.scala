package dm.content

import common.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenerateTmSingerRsiD {
  private val localRun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetastoreUris: String = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDatabase: String = ConfigUtils.HIVE_DATABASE
  private var sparkSession: SparkSession = _

  private val mysqlUser: String = ConfigUtils.MYSQL_USER
  private val mysqlUrl: String = ConfigUtils.MYSQL_URL
  private val mysqlPassword: String = ConfigUtils.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      println(s"请输入数据日期,格式例如：年月日(20201231)")
      System.exit(1)
    }

    if(localRun){
      sparkSession = SparkSession.builder().master("local").appName("Generate_TM_Song_Rsi_D")
        .config("spark.sql.shuffle.partitions","1")
        .config("hive.metastore.uris",hiveMetastoreUris).enableHiveSupport().getOrCreate()
      sparkSession.sparkContext.setLogLevel("Error")
    }else{
      sparkSession = SparkSession.builder().appName("Generate_TM_Song_Rsi_D").enableHiveSupport().getOrCreate()
    }

    val currentDate = args(0)

    sparkSession.sql(s"use $hiveDatabase ")
    val dataFrame = sparkSession.sql(
      s"""
         | select
         |   data_dt,
         |   SINGER1ID,
         |   SINGER1,
         |   sum(SING_CNT) as SING_CNT,
         |   sum(SUPP_CNT) as SUPP_CNT,
         |   sum(RCT_7_SING_CNT) as RCT_7_SING_CNT,
         |   sum(RCT_7_SUPP_CNT) as RCT_7_SUPP_CNT,
         |   sum(RCT_7_TOP_SING_CNT) as RCT_7_TOP_SING_CNT,
         |   sum(RCT_7_TOP_SUPP_CNT) as RCT_7_TOP_SUPP_CNT,
         |   sum(RCT_30_SING_CNT) as RCT_30_SING_CNT,
         |   sum(RCT_30_SUPP_CNT) as RCT_30_SUPP_CNT,
         |   sum(RCT_30_TOP_SING_CNT) as RCT_30_TOP_SING_CNT,
         |   sum(RCT_30_TOP_SUPP_CNT) as RCT_30_TOP_SUPP_CNT
         | from TW_SONG_FTUR_D
         | where data_dt = ${currentDate}
         | GROUP BY data_dt,SINGER1ID,SINGER1
      """.stripMargin)

    import org.apache.spark.sql.functions._
    /**
     *   日周期-整体影响力
     *   7日周期-整体影响力
     *   30日周期-整体影响力
     */
    dataFrame.withColumn("RSI_1D",pow(
      log(col("SING_CNT")/1+1)*0.63*0.8+log(col("SUPP_CNT")+1)*0.63*0.2
      ,2)*10)
      .withColumn("RSI_7D",pow(
        (log(col("RCT_7_SING_CNT")/7+1)*0.63+log(col("RCT_7_TOP_SING_CNT")+1)*0.37)*0.8
          +
          (log(col("RCT_7_SUPP_CNT")/7+1)*0.63+log(col("RCT_7_TOP_SUPP_CNT")+1)*0.37)*0.2
        ,2)*10)
      .withColumn("RSI_30D",pow(
        (log(col("RCT_30_SING_CNT")/30+1)*0.63+log(col("RCT_30_TOP_SING_CNT")+1)*0.37)*0.8
          +
          (log(col("RCT_30_SUPP_CNT")/30+1)*0.63+log(col("RCT_30_TOP_SUPP_CNT")+1)*0.37)*0.2
        ,2)*10).createTempView("TEMP_TW_SONG_FTUR_D")

    val rsi_1d = sparkSession.sql(
      s"""
         | select
         |  "1" as PERIOD,SINGER1ID,SINGER1,RSI_1D as RSI,
         |  row_number() over(partition by data_dt order by RSI_1D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)
    val rsi_7d = sparkSession.sql(
      s"""
         | select
         |  "7" as PERIOD,SINGER1ID,SINGER1,RSI_7D as RSI,
         |  row_number() over(partition by data_dt order by RSI_7D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)
    val rsi_30d = sparkSession.sql(
      s"""
         | select
         |  "30" as PERIOD,SINGER1ID,SINGER1,RSI_30D as RSI,
         |  row_number() over(partition by data_dt order by RSI_30D desc) as RSI_RANK
         | from TEMP_TW_SONG_FTUR_D
      """.stripMargin)

    rsi_1d.union(rsi_7d).union(rsi_30d).createTempView("result")

    sparkSession.sql(
      s"""
         |insert overwrite table TW_SINGER_RSI_D partition(data_dt=${currentDate}) select * from result
      """.stripMargin)

    /**
     * 这里把 排名前30名的数据保存到mysql表中
     */
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    sparkSession.sql(
      s"""
         | select ${currentDate} as data_dt,PERIOD,SINGER1ID,SINGER1,RSI,RSI_RANK from result where rsi_rank <=30
      """.stripMargin).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl,"tm_singer_rsi",properties)
    println("**** all finished ****")
  }
}
