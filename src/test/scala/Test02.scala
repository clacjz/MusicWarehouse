import common.ConfigUtils
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test02{
  val getNameLength = (s : String) => {
    s.length
  }
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    import session.implicits._
    val list = List[String]("zhangsan", "lisi", "wangwu")

    val dataFrame: DataFrame = list.toDF("name")

    val myUdf = udf(getNameLength)

    session.udf.register("xxxudf", (s : String) => {
      s.length
    })

    dataFrame.withColumn("age", lit(18))
      .withColumn("nameLength", myUdf(col("name")))
      .show()

  }
}
