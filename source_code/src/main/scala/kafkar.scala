import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.concurrent._
import java.util.concurrent.atomic._

object kafkar {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    // setup streaming context
    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkar")
    val ssc = new StreamingContext(conf, Seconds(2))

    //Kafka topic name: 'test'. The val: kafkaStream created here itself is an RDD.
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("test35" -> 5))

    //Extracting the Address fields from each excel line.
    val address_fields = kafkaStream.map(x => {val arr = x.toString().split(","); arr(3).trim })
    //Extracting the Time fields from each excel line.
    val time_fields = kafkaStream.map(x => {val arr = x.toString().split(","); arr(2).trim })

    //Tranforming the Address Fields
    val address_mapped_fields = address_fields.map(word => (word, 1))
    val address_reduced_values = address_mapped_fields.reduceByKey((x,y)=>x+y)
    val address_sorted_values = address_reduced_values.transform(rdd => rdd.sortBy(x => x._2, false))

    //Tranforming the Time Fields
    val time_transform_fields = time_fields.map(x => {val arr = x.toString().split(" "); arr(0) +":"+arr(1) })
    val time_transform2_fields = time_transform_fields.map(x => {val arr = x.toString().split(":"); arr(0) + arr(3) })
    val time_mapped_fields = time_transform2_fields.map(word => (word, 1))
    val time_reduced_values = time_mapped_fields.reduceByKey((x,y)=>x+y)
    val time_sorted_values = time_reduced_values.transform(rdd => rdd.sortBy(x => x._2, false))


    println("Address Values")
    address_sorted_values.print()
    println("Time Values")
    time_sorted_values.print()

    ssc.checkpoint("C:/checkpoint/")
    ssc.start
    ssc.awaitTermination()
  }
}