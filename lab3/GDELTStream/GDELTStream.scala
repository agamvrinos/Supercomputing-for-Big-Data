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
  val OUTPUT_TOPIC = "test-output"    //TODO: CHANGE TO "gdelt-histogram"

  // StateStores names
  val HISTOGRAM_STORE_NAME = "histogram"

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder


  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // filter lines with length less than 23 (after tab split)
  val filtered:KStream[String, Int] = records
    .filter((key, line) => line.split(TAB_SPLIT, -1).length > 23)
    .map((key, line) => {
      val linePair = line.split(TAB_SPLIT, -1)
      val rawNames = linePair(23)
      (rawNames, 1)
    }).filter((names, count) => !names.isEmpty)

  println("processing...")

  val valid:KStream[String, Int] = filtered.filter((names, count) => !names.isEmpty)

  valid.to("test-output")

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

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    ("Donald Trump", 1L)
  }

  // Close any resources if any
  def close() {
  }
}
