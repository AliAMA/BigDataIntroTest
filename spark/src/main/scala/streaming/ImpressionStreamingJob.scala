package streaming

import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector._
import domain.{ClickEvent, ImpressionEvent}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._

/**
  * Created by Ali on 2/3/2019.
  */
object ImpressionStreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Streaming with Spark")
    val sqlContext = getSQLContext(sc)

    val batchSeconds = 4

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val kafkaParams = Map(
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest")

      val kstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Map("app-impressions" -> 1), StorageLevel.MEMORY_AND_DISK)
        .map(_._2)

      val stream = kstream.transform { input =>
        val inputRDD = input.flatMap { line =>
          val record = line.replace("\n", "").split("\\t")
          println("@254: " + line)
          if (record.length == 7)
          // make sure we have complete records
            Some(ImpressionEvent(record(0), record(1), record(2), record(3).toDouble, record(4), record(5)
              , record(6).toLong))
          else
            None
        }

        //val df = inputRDD.toDF()
        //df.registerTempTable("clicks")
        //sqlContext.cacheTable("clicks")

        inputRDD
      }.cache()

      stream.foreachRDD(rdd => {
        rdd
          .saveToCassandra("tapsell", "impressions")
      })

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, Seconds(batchSeconds))
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()
  }
}
