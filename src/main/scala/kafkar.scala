import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkar {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[*]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(2))

    //Kafka topic name: 'test'
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("test" -> 5))

    kafkaStream.print()
    ssc.start
    ssc.awaitTermination()

  }
}