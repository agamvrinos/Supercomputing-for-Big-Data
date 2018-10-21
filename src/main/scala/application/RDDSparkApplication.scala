package application

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class DateRecordPair(data: String, result: ArrayBuffer[Result])
case class Result(topic: String, count: Int)

object RDDSparkApplication {
  var COMMA_SPLIT = ","
  var SEMICOLON_SPLIT = ";"
  var TAB_SPLIT = "\t"
  var IGNORED_TOPICS = Array(
    "Type ParentCategory",
    "CategoryType ParentCategory",
    "Read Full",
    "Read Full Blog"
  )
  var CUSTOM_DATE_FORMAT = "yyyy-MM-dd"
  var RAW_DATE_FORMAT = "yyyyMMddHHmmss"

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat(RAW_DATE_FORMAT, Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat(CUSTOM_DATE_FORMAT)

    val session = SparkSession
      .builder
      .appName("RDDSparkApplication")
      .getOrCreate()

    val sc = session.sparkContext

    val filePathsRDD: RDD[String] = sc.textFile("s3://gdelt-open-data/v2/gkg/*.gkg.csv")

    // filter lines with length less than 23 (after tab split)
    val filtered = filePathsRDD.filter(line => line.split(TAB_SPLIT, -1).length > 23)

    val dateToStringDataRDD: RDD[(String, String)] = filtered.map ( line => {
      val linePair = line.split(TAB_SPLIT, -1)
      val rawDate = linePair(1)
      val rawNames = linePair(23)
      val formattedDate:String = dateFormat.format(format.parse(rawDate))
      (formattedDate, rawNames)
    })

    // filter out the corrupted lines
    val validPairRDDs = dateToStringDataRDD.filter(x => !x._2.isEmpty)

    // create (date, (topic, 1)) pairs
    val valuesSplittedRdd = validPairRDDs
      .flatMapValues(line => line.split(SEMICOLON_SPLIT, -1))
      .mapValues(line => (line.split(COMMA_SPLIT, -1)(0), 1))

    // filter out the false positives
    val filteredTopicsRdd = valuesSplittedRdd.filter(pair => !IGNORED_TOPICS.contains(pair._2._1))

    // group by date and sum
    val groupedByDateRDDs = filteredTopicsRdd.map{ case (date, (topic, value)) => (date, Map(topic -> value)) }.
      reduceByKey{ (acc, m) =>
        acc ++ m.map{ case (n, v) => n -> (acc.getOrElse(n, 0) + v) }
      }

    // sort them and keep the first 10
    val sortedRDD = groupedByDateRDDs.mapValues(x => x.toList.sortBy(x => -x._2).take(10))

    // val parallelizedJsonArrayBuffer = sc.parallelize(serializeResultsToJSON(sortedRDD.collect())) // In case we want to output this with a specific JSON schema
    val sortedDataframe = session.createDataFrame(sortedRDD.collect())

    sortedDataframe.write.mode(SaveMode.Overwrite).json("s3a://ohio-sbd-bucket/output")

    session.stop()
  }

  private def serializeResultsToJSON(results: Array[(String, List[(String, Int)])]): ArrayBuffer[DateRecordPair] = {
    val container = new ArrayBuffer[DateRecordPair]()

    for (tuple <- results) {
      val l = new ArrayBuffer[Result]()
      for (topicCountPair <- tuple._2) {
        val result = Result(topicCountPair._1, topicCountPair._2)
        l.append(result)
      }

      container.append(DateRecordPair(tuple._1, l))
    }
    container
  }
}
