package kr.re.keti

import java.net.InetAddress
import java.time.LocalDateTime
import java.util.Date
import java.util.concurrent.TimeUnit

import com.mongodb.BasicDBObject
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import scalaj.http.Http

object MongoDBRW {
  var host : String = _

  val spark : SparkSession = SparkSession.builder().getOrCreate()
  val sc : SparkContext = spark.sparkContext

  def getRecentFcstSejong(queryTime : String): DataFrame = {
    val d = MongoSpark.load(sc, ReadConfig(Map("uri" -> s"mongodb://$host/sites.fcst_production_sejong_2")))
    val aggregatedRdd = d.withPipeline(Seq(
      Document.parse("{ $match: { CRTN_TM: \""+queryTime+"\" }}")
    ))

    aggregatedRdd.toDF()
  }

  def getRecentFcstEco(queryTime : String): DataFrame = {
    val d = MongoSpark.load(sc, ReadConfig(Map("uri" -> s"mongodb://$host/sites.fcst_production_echo")))
    val aggregatedRdd = d.withPipeline(Seq(
      Document.parse("{ $match: { CRTN_TM: \""+queryTime+"\" }}")
    ))

    aggregatedRdd.toDF()
  }

  def getRecentSiteList(): DataFrame = {

    val d = MongoSpark.load(sc, ReadConfig(Map("uri" -> s"mongodb://$host/sites.sitesList")))

    val aggregatedRdd = d.withPipeline(Seq(
      Document.parse("{$sort:{CRTN_TM: -1}}"),
      Document.parse("{$limit: 1}")
    ))

    aggregatedRdd.toDF()
  }

  def loggingErr(msg : String, collection : String) : Unit = {

    val endpoint = s"mongodb://$host/sites.SparkStreaming_logs"
    val writeConfig = WriteConfig(Map("uri" -> endpoint))

    // Save Err msg in mongodb
    val date = LocalDateTime.now()
    val from = InetAddress.getLocalHost.getHostAddress

    val logMsgDBO = new BasicDBObject()
      .append("date", date)
      .append("log", s"""from : $from, to : mongodb://$host/SparkStreaming_logs, subject : KETI_Phase1(ERR_MSG), action : Insert, status : false, message:$msg""")

    val logMsg = sc.parallelize(Seq(logMsgDBO))
    MongoSpark.save(logMsg, writeConfig)

    // Send Event to CEP Module via http
    val bodyString = s"""'{"gen_date": "$date", "from" : "$from", "to" : "mongodb://$host/$collection", "subject" : "KETI_Phase1(ERR_MSG)", "action" : "Insert", "status" : "false", "message" : "$msg"}'"""

    Tools.printInfo(Http("http://10.0.0.50:5555")
      .charset("UTF-8")
      .header("content-type", "application/json")
      .postData(s"""[{"body": $bodyString}]""")
      .asString.toString)

  }

  def insertDB(rdd: DataFrame, db : String, collection: String): Unit = {
    val start_time = System.nanoTime()

    // Save Data
    val endpoint = s"mongodb://$host/$db.$collection"
    val writeConfig = WriteConfig(Map("uri" -> endpoint))
    MongoSpark.save(rdd, writeConfig)

    // Send Event to CEP Module via http
    val rdd_count = rdd.count()
    val date = LocalDateTime.now()
    val from = InetAddress.getLocalHost.getHostAddress

    val bodyString = s"""'{"gen_date": "$date", "from" : "$from", "to" : "mongodb://$host/$collection", "subject" : "KETI_Phase1($collection)", "action" : "Insert", "status" : "$rdd_count"}'"""

    Tools.printInfo(Http("http://10.0.0.50:5555")
      .charset("UTF-8")
      .header("content-type", "application/json")
      .postData(s"""[{"body": $bodyString}]""")
      .asString.toString)
  }
}
