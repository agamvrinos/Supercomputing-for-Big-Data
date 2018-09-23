package application

import java.io.InputStream
import java.text.SimpleDateFormat

import DTO.GDeltData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, SparkSession}

import scala.io.Source

object DataframeSparkApplication {

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

  def main(args: Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat(CUSTOM_DATE_FORMAT)

    val schema = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("date", TimestampType, nullable = true),
        StructField("sourceCollectionId", IntegerType, nullable = true),
        StructField("sourceCommonNames", StringType, nullable = true),
        StructField("documentIdentifier", StringType, nullable = true),
        StructField("counts", StringType, nullable = true),
        StructField("v2Counts", StringType, nullable = true),
        StructField("themes", StringType, nullable = true),
        StructField("v2Themes", StringType, nullable = true),
        StructField("locations", StringType, nullable = true),
        StructField("v2Locations", StringType, nullable = true),
        StructField("persons", StringType, nullable = true),
        StructField("v2Persons", StringType, nullable = true),
        StructField("organizations", StringType, nullable = true),
        StructField("v2Organizations", StringType, nullable = true),
        StructField("v2Tone", StringType, nullable = true),
        StructField("dates", StringType, nullable = true),
        StructField("gCam", StringType, nullable = true),
        StructField("sharingImages", StringType, nullable = true),
        StructField("relatedImages", StringType, nullable = true),
        StructField("socialImageEmbeds", StringType, nullable = true),
        StructField("socialVideoEmbeds", StringType, nullable = true),
        StructField("quotations", StringType, nullable = true),
        StructField("allNames", StringType, nullable = true),
        StructField("amounts", StringType, nullable = true),
        StructField("translationInfo", StringType, nullable = true),
        StructField("extras", StringType, nullable = true)
      )
    )

    val stream: InputStream = getClass.getResourceAsStream(CONFIG_FILE_PATH)
    val lines: List[String] = (for (line <- Source.fromInputStream( stream , "UTF16").getLines) yield line).toList

    val session = SparkSession
      .builder
      .appName("SparkProject")
      .master("local")
      .getOrCreate()

    import session.implicits._
    val df = session.read
      .schema(schema)
      .option("sep", "\t")
      .option("timestampFormat", "yyyyMMddS")
      .csv(lines:_*)
      .as[GDeltData]

    // filter out the records with null values at the allNames column
    val validData = df.filter("allNames is not null")

    // keep the columns we are interested in and format the date
    val dateAllNamesRecords: Dataset[(String, String)] = validData.map(x => (dateFormat.format(x.date), x.allNames))

    // split the merged topics
    val dateMultiTopicRecords = dateAllNamesRecords.flatMap {case (x1, x2) => x2.split(";").map((x1, _))}

    // rename columns for clarity
    var dateMultiTopicRecordsRenamed = dateMultiTopicRecords.withColumnRenamed("_1", "date")
    dateMultiTopicRecordsRenamed = dateMultiTopicRecordsRenamed.withColumnRenamed("_2", "topicCount")

    // create (topic, 1) pairs
    val dateSingleTopicRecords = dateMultiTopicRecordsRenamed
      .withColumn("word", split($"topicCount", ",").getItem(0))
      .withColumn("count", lit(1))

    // drop redundant column
    val dateSingleTopicRecordsSplit= dateSingleTopicRecords.drop("topicCount")

    // filter the ignored topics
    var filteredTopics = dateSingleTopicRecordsSplit.filter(!col("word").isin(IGNORED_TOPICS :_*))

    // group results by date and topic in order to summarize the counts
    val aggregated = filteredTopics.groupBy("date", "word").agg(sum($"count") as "Total")

    // order results by count
    val sorted = aggregated.orderBy(desc("Total"))

    sorted.show(10)

    session.stop()
  }
}
