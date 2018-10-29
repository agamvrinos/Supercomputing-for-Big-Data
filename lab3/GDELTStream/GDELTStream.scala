package lab3

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object GDELTStream extends App {
  import Serdes._

  // Application constants
  var TAB_SPLIT = "\t"
  var COMMA_SPLIT = ","
  var SEMICOLON_SPLIT = ";"
  var IGNORED_TOPICS = Array(
    "Type ParentCategory",
    "CategoryType ParentCategory",
    "Read Full",
    "Read Full Blog"
  )

  // Topics
  val INPUT_TOPIC = "gdelt"
  val OUTPUT_TOPIC = "gdelt-histogram"

  // StateStores names
  val HISTOGRAM_STORE_NAME = "histogram"
  val RECORDED_TIMESTAMPS_STORE_NAME = "recorded"

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Initialize and add StateStores
  val histogramStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(HISTOGRAM_STORE_NAME), Serdes.String, Serdes.Long)
  builder.addStateStore(histogramStoreBuilder)
  val outdatedStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(RECORDED_TIMESTAMPS_STORE_NAME), Serdes.String, Serdes.Long)
  builder.addStateStore(outdatedStoreBuilder)

  val records: KStream[String, String] = builder.stream[String, String](INPUT_TOPIC)

  // Filter lines with length < 23 and empty record names
  val filtered:KStream[String, String] = records
    .filter((key, line) => line.split(TAB_SPLIT, -1).length > 23)
    .map((key, line) => {
      val linePair = line.split(TAB_SPLIT, -1)
      val rawNames = linePair(23)
      (key, rawNames)
    }).filter((_, names) => !names.isEmpty)

  // Flatten to generate (default_key, name) pairs
  val splitNames = filtered
    .flatMapValues(names => names.split(SEMICOLON_SPLIT))
    .map((key, name) => (key, name.split(COMMA_SPLIT)(0)))

  // Remove false positives
  val filteredTopics = splitNames.filter((_, name) => !IGNORED_TOPICS.contains(name))

  // Transform the stream of records and save the generated stream to the output topic
  val results:KStream[String, Long] = filteredTopics.transform(new HistogramTransformer, HISTOGRAM_STORE_NAME, RECORDED_TIMESTAMPS_STORE_NAME)
  results.to(OUTPUT_TOPIC)

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

class HistogramTransformer extends Transformer[String, String, (String, Long)] {

  val MILLIS_PER_HOUR = 60 * 60 * 1000L

  // State Store names
  val STORE_NAME = "histogram"
  val RECORDED_TIMESTAMPS_STORE_NAME = "recorded"
  // Delimiter for the pseudo-key generation
  val CUSTOM_DELIMITER = ";"

  var context: ProcessorContext = _
  var topicCountsStore: KeyValueStore[String, Long] = _
  var recordsTimestampStore: KeyValueStore[String, Long] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.topicCountsStore = context.getStateStore(STORE_NAME).asInstanceOf[KeyValueStore[String, Long]]
    this.recordsTimestampStore = context.getStateStore(RECORDED_TIMESTAMPS_STORE_NAME).asInstanceOf[KeyValueStore[String, Long]]

    //TODO: Change to 1 hour
    context.schedule(TimeUnit.SECONDS.toMillis(10), PunctuationType.WALL_CLOCK_TIME, outdatedRecordsCheck(_))
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    val recordTimestamp: Long = context.timestamp()

    println("=========================================================================================")
    println("Processing key: " + key + ", name: " + name)

    // Insert/Update the count for a specific topic
    val recordCount = this.topicCountsStore.get(name)
    println("Count = " + recordCount)
    println("=========================================================================================")
    if (recordCount == 0) {
      this.topicCountsStore.put(name, 1)
    } else {
      this.topicCountsStore.put(name, recordCount + 1)
    }

    val pseudoKey = generatePseudoKey(name, recordTimestamp)

    // Insert/Update the count for a specific topic at a specific timestamp
    val recordTimestampCount = this.recordsTimestampStore.get(pseudoKey)
    if (recordTimestampCount == 0) {
      this.recordsTimestampStore.put(pseudoKey, 1)
    } else {
      this.recordsTimestampStore.put(pseudoKey, recordTimestampCount + 1)
    }

    (name, this.topicCountsStore.get(name))
  }

  /**
    * Since the timestamp for each record is not unique, a pseudo key
    * of the format name;timestamp is used to count the occurrences of
    * a name at a particular timestamp.
    *
    * @param name the topic's name
    * @param recordsTimestamp the record;s timestamp
    * @return the generated pseudo-key
    */
  def generatePseudoKey(name: String, recordsTimestamp:Long): String = {
    name + CUSTOM_DELIMITER + recordsTimestamp
  }

  /**
    * Iterates over the "recordsTimestampStore" StateStore holding information about
    * the timestamp at which a record was processed and detects outdated entries. If
    * an outdated entry is found, the pair's value is used to decrement the corresponding
    * topic name in the "topicCountsStore" StateStore
    *
    * @param timestamp the timestamp when the method was called
    */
  def outdatedRecordsCheck(timestamp: Long): Unit = {
    println("****************** INITIATING  ******************")
    val df:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    val date:String = df.format(timestamp)
    println("Running at \"" + date + "\"")

    val currentTimestamp = System.currentTimeMillis()

    val recordsTimestampStoreEntries = recordsTimestampStore.all()

    while (recordsTimestampStoreEntries.hasNext) {
      val next = recordsTimestampStoreEntries.next
      val topicTimestampMergedKey: String = next.key
      val splitArray = topicTimestampMergedKey.split(CUSTOM_DELIMITER)

      val recordsTimestamp: Long = splitArray(1).toLong

      println("Current Timestamp: " + df.format(currentTimestamp))
      println("Record Timestamp: " + df.format(recordsTimestamp))

      println(currentTimestamp - recordsTimestamp)
      if (currentTimestamp - recordsTimestamp > MILLIS_PER_HOUR  ) {

        val outdatedRecordCount = next.value
        val topicName = splitArray(0)

        println("Found outdated topic: \"" + topicName + "\"")

        val oldCount = topicCountsStore.get(topicName)
        val newCount = oldCount - outdatedRecordCount
        println("Old count = " + oldCount)
        println("New count = " + newCount)
        topicCountsStore.put(topicName, newCount)

        context.forward(topicName, newCount, To.all())
        context.commit()

        recordsTimestampStore.delete(topicTimestampMergedKey)
      }
    }
    println("****************** ENDING  ******************")
  }

  // Close any resources if any
  def close() {
  }
}
