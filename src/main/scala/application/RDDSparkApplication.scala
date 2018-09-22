package application

import java.io._
import java.text.SimpleDateFormat
import java.util.Locale

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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
  var CONFIG_FILE_PATH: String = "/local_index.txt"
  var CUSTOM_DATE_FORMAT = "yyyy-MM-dd"
  var RAW_DATE_FORMAT = "yyyyMMddHHmmss"

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat(RAW_DATE_FORMAT, Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat(CUSTOM_DATE_FORMAT)

    val session = SparkSession
      .builder
      .appName("RDDSparkProject")
      .master("local")
      .getOrCreate()

    val sc = session.sparkContext

    val stream: InputStream = getClass.getResourceAsStream(CONFIG_FILE_PATH)
    val lines: String = Source.fromInputStream( stream , "UTF16").getLines.mkString(COMMA_SPLIT)
    val filePathsRDD: RDD[String] = sc.textFile(lines)

    val dateToStringDataRDD: RDD[(String, String)] = filePathsRDD.map ( line => {
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

    // group the (date, (topic, 1) by date
    val groupedByDateRDDs =  filteredTopicsRdd.groupByKey()

    // calculate the sum for each date and for each topic
    val groupedByDateSummedRDDs = groupedByDateRDDs.mapValues(x =>
      x.groupBy(_._1).mapValues(x => x.map(_._2).sum).toList)

    // sort them and keep the first 10
    val sortedRDD = groupedByDateSummedRDDs.mapValues(x => x.sortBy(x => -x._2).take(10))

    val jsonResults: String = serializeResultsToJSON(sortedRDD.collect())
    saveJSONResultsToFile(jsonResults)

    sc.stop()
  }

  private def serializeResultsToJSON(results: Array[(String, List[(String, Int)])]): String = {
    val container = new ArrayBuffer[DateRecordPair]()

    for (tuple <- results) {
      val l = new ArrayBuffer[Result]()
      for (topicCountPair <- tuple._2) {
        val result = Result(topicCountPair._1, topicCountPair._2)
        l.append(result)
      }

      container.append(DateRecordPair(tuple._1, l))
    }
    implicit val formats = DefaultFormats
    val jsonString = write(container)
    jsonString
  }

  private def saveJSONResultsToFile(jsonResults: String): Unit = {
    val file = new File("results.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(jsonResults)
    bw.close()
  }
}
