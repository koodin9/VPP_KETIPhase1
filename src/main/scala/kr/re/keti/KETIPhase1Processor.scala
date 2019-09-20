package kr.re.keti

import java.sql.Timestamp
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession

object KETIPhase1Processor {

  val conf : SparkConf = new SparkConf()
    .setAppName("KETI_Phase1")
    //    .set("spark.sql.streaming.schemaInference", "true")
    //.setMaster("local[*]")
    .setMaster("yarn")

  val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
  val rootLogger : Logger = Logger.getRootLogger


  def main(args: Array[String]){

    import spark.implicits._
    val toString = udf((payload: Array[Byte]) => new String(payload))

    rootLogger.setLevel(Level.WARN)
    rootLogger.warn(args.length)
    if(args.length != 2){

      rootLogger.error("spark-submit ./KETI_Phase1.jar [yyyyMMddHHmm] [MongoDB Host]")
      rootLogger.error("spark-submit ./KETI_Phase1.jar 201909041500 10.0.1.40")
      System.exit(1)
    }
    val crtn_tm = args(0)
    MongoDBRW.host = args(1)


    producing_keti_phase_1(crtn_tm)
  }
  def fcstHourList: UserDefinedFunction = udf((start: String) => {
    val iformat = new java.text.SimpleDateFormat("yyyyMMddHHmm")
    val time = iformat.parse(start)
    val cal = Calendar.getInstance()
    cal.setTime(time)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)

    var retStr = ""
    for (i <- 0 to 48) {
      cal.add(Calendar.HOUR, i)
      if(i==0) retStr = retStr.concat(iformat.format(cal.getTime))
      else retStr = retStr.concat(","+iformat.format(cal.getTime))
      cal.add(Calendar.HOUR, -i)
    }
    retStr
  })

  val timestamp_diff : UserDefinedFunction = udf((startTime: Timestamp, endTime: Timestamp) => {
    ((startTime.getTime - endTime.getTime) / 3600000 ).intValue.toString
  })

  def producing_keti_phase_1(queryDateString : String) : Unit = {
    import spark.implicits._

    val recentEchoData = MongoDBRW.getRecentFcstEco(queryDateString)
    val recentSejongData = MongoDBRW.getRecentFcstSejong(queryDateString)

    val tmp = MongoDBRW.getRecentSiteList()

    val first_row = sc.parallelize(Seq(tmp.sort(desc("CRTN_TM")).first()))

    val recentSiteList = spark.createDataFrame(first_row, tmp.schema)
      .sort(desc("CRTN_TM"))
      .withColumn("expcol", explode($"sites_list"))
      .withColumn("CRTN_TM", lit(queryDateString))
      //      .filter($"CRTN_TM"=== queryDateString)
      .withColumn("_id", $"expcol.site")
      .withColumn("FCST_TM_LIST", fcstHourList($"CRTN_TM"))
      .withColumn("FCST_TM", explode(split($"FCST_TM_LIST", ",")))
      .withColumn("LEAD_HR", timestamp_diff(
        unix_timestamp(col("FCST_TM"), "yyyyMMddHHmm").cast("timestamp"),
        unix_timestamp(col("CRTN_TM"), "yyyyMMddHHmm").cast("timestamp")
      ))
      .withColumn("COMPX_ID",$"_id")
      .select("_id","CRTN_TM","COMPX_ID","FCST_TM","LEAD_HR")

    val joindf =
      recentSiteList.as("boilerplate").join(recentEchoData.as("echo"), Seq("CRTN_TM", "COMPX_ID", "FCST_TM"),"left_outer")
        .select("COMPX_ID","CRTN_TM","FCST_TM","boilerplate.LEAD_HR","echo.FCST_QGEN")
        .withColumnRenamed("FCST_QGEN","ECHO_FCST_QGEN")
        .join(recentSejongData.as("sejong"), Seq("CRTN_TM", "COMPX_ID", "FCST_TM"),"left_outer")
        .withColumnRenamed("FCST_QGEN","SEJONG_FCST_QGEN")
        .withColumn("OUT_FCST_QGEN",
          when(col("ECHO_FCST_QGEN").isNotNull, col("ECHO_FCST_QGEN"))
            .otherwise(col("SEJONG_FCST_QGEN"))
        )
        .withColumn("OUT_FCST_QGEN",
          when(col("OUT_FCST_QGEN").isNull, -1.0)
            .otherwise(col("OUT_FCST_QGEN"))
        )
        .withColumn("OUT_LEAD_HR", $"boilerplate.LEAD_HR".cast(StringType))
        .sort(asc("COMPX_ID"),asc("OUT_LEAD_HR"))
        .withColumn("_id",$"COMPX_ID")
        .select("_id","COMPX_ID","CRTN_TM","FCST_TM","OUT_LEAD_HR","OUT_FCST_QGEN")
        .withColumnRenamed("OUT_LEAD_HR","LEAD_HR")
        .withColumnRenamed("OUT_FCST_QGEN","FCST_QGEN")
        .groupBy("_id", "CRTN_TM")
        .agg(collect_list(struct("COMPX_ID","CRTN_TM","FCST_TM","LEAD_HR","FCST_QGEN")).alias("records"))
        .sort(asc("_id"))
        .withColumnRenamed("_id","COMPX_ID")
    MongoDBRW.insertDB(joindf, "sites", "keti_phase_1")
  }
}