package common

object StringUtils {
  /**
   * 检查字符串是否为空
   * @param str
   * @return
   */
  def checkString(str:String) = {
    if(str == null || "".equals(str)) "" else str
  }
}
