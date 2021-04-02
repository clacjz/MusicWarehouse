package base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class PairRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {


  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val str: String = key.asInstanceOf[String]
    str
  }

  override def generateActualKey(key: Any, value: Any): Any = {
    null
  }
}
