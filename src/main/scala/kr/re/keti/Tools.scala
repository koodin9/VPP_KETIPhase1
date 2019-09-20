package kr.re.keti

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Tools {
  def printInfo(info: String): Unit = {
    println(s"${currentTimeToString()}: $info")
  }

  def currentTimeToString(): String = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
  }

}
