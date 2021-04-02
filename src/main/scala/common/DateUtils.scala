package common

import java.text.SimpleDateFormat
import java.math.BigDecimal
import java.util.{Calendar, Date}

object DateUtils {
  def formatDate(stringDate : String) : String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var formatDate = ""
    try {
      formatDate = format.format(format.parse(stringDate))
    } catch {
      case e : Exception => {
        try {
          val bigDecimal = new BigDecimal(stringDate)
          val date = new Date(bigDecimal.longValue())
          formatDate = format.format(date)
        } catch {
          case e : Exception => {
            formatDate
          }
        }
      }
    }
    formatDate
  }

  /**
   * 获取输入日期的前几天的日期
   * @param currentDate yyyyMMdd
   * @param i 获取前几天的日期
   * @return yyyyMMdd
   */
  def getCurrentDatePreDate(currentDate:String,i:Int) = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date: Date = sdf.parse(currentDate)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE,-i)
    val per7Date = calendar.getTime
    sdf.format(per7Date)
  }
}
