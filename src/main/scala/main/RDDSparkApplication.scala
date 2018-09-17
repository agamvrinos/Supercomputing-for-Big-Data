package main

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDSparkApplication {
  var COMMA_SPLIT = ","
  var SEMICOLON_SPLIT = ";"
  var TAB_SPLIT = "\t"
  var CONFIG_FILE_PATH: String = "/local_index.txt"

  var CUSTOM_DATE_FORMAT = "dd/mm/yy"
  var RAW_DATE_FORMAT = "yyyyMMddS"

  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat(RAW_DATE_FORMAT, Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat(CUSTOM_DATE_FORMAT)

    val session = SparkSession
      .builder
      .appName("SparkProject")
      .master("local")
      .getOrCreate()

    val sc = session.sparkContext

    //====================================================================================
    val stream: InputStream = getClass.getResourceAsStream(CONFIG_FILE_PATH)
    val lines: String = Source.fromInputStream( stream , "UTF16").getLines.mkString(COMMA_SPLIT)
    val filePathsRDD: RDD[String] = sc.textFile(lines)
    //====================================================================================

    val dateToStringDataRDD: RDD[(String, String)] = filePathsRDD.map ( line => {
      val linePair = line.split(TAB_SPLIT, -1)
      val rawDate = linePair(1)
      val linepr = linePair(23)

      val formattedDate = dateFormat.format(format.parse(rawDate))
      (formattedDate, linepr)
    })

    val pairs2 = dateToStringDataRDD.filter(x => !x._2.isEmpty)
    pairs2.collect().foreach(println)

    val valuesSplittedRdd = pairs2
      .flatMapValues(line => {
        line.split(SEMICOLON_SPLIT, -1)
      })
      .mapValues(line => (line.split(COMMA_SPLIT, -1)(0), line.split(COMMA_SPLIT, -1)(1).toInt))

    var pairs1 = valuesSplittedRdd.collect()
    pairs1.foreach(println)

    val aoua =  valuesSplittedRdd.groupByKey()
//    var pairs2 = aoua.collectAsMap()
//    pairs2.foreach(println)

    val aoua3 = aoua.mapValues(x => x.groupBy(_._1).mapValues(x => x.map(_._2).sum).toList)    // Single date, data tuple
    aoua3.foreach(println)

    sc.stop()
  }
}
