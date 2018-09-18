package application

import java.io.{File, InputStream, PrintWriter}
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

  var CUSTOM_DATE_FORMAT = "dd/MM/yyyy"
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

      val formattedDate = dateFormat.format(format.parse(rawDate))
      (formattedDate, rawNames)
    })

    val validPairRDDs = dateToStringDataRDD.filter(x => !x._2.isEmpty)   // filter out the corrupted lines

    val valuesSplittedRdd = validPairRDDs
      .flatMapValues(line => line.split(SEMICOLON_SPLIT, -1))
      .mapValues(line => (line.split(COMMA_SPLIT, -1)(0), line.split(COMMA_SPLIT, -1)(1).toInt))

    val groupedByDateRDDs =  valuesSplittedRdd.groupByKey()
    val groupedByDateSummedRDDs = groupedByDateRDDs.mapValues(x => x.groupBy(_._1).mapValues(x => x.map(_._2).sum).toList)    // Single date, data tuple

    // REMOVE ME
    printResults(groupedByDateSummedRDDs.collect())

    sc.stop()

  }

  /**
    * Saves the values of the "results" parameter for in "results.txt"
    * TESTING purposes.
    * @param results the grouped by date results
    */
  def printResults(results: Array[(String, List[(String, Int)])]): Unit = {
    val pw = new PrintWriter(new File("results.txt" ))

    for (res <- results) {
      var str: String =  res._1 + " \n=====================================\n"
      for (res2 <- res._2) {
        str += res2._1 + ": " + res2._2 + "\n"
      }
      pw.write(str)
    }

    pw.close
  }
}
