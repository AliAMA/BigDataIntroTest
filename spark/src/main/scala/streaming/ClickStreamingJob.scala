package streaming

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.sql.SaveMode
import _root_.kafka.serializer.StringDecoder
import domain.ClickEvent
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.WriteConf

/**
  * Created by Ali on 2/3/2019.
  */
object ClickStreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Streaming with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchSeconds = 4

    /*
    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    //ssc.remember(Minutes(5))
    ssc.start()
    //ssc.awaitTermination()
    */
    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val kafkaParams = Map(
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest")

      val kstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Map("web-clicks" -> 1), StorageLevel.MEMORY_AND_DISK)
        .map(_._2)

      val stream = kstream.transform { input =>
        val inputRDD = input.flatMap { line =>
          val record = line.replace("\n", "").split("\\t")
          println("@254: " + line)
          println("@254: " + record.length)
          println("@254: " + record(0))
          println("@254: " + record(1))
          if (record.length == 2)
          // make sure we have complete records
            Some(ClickEvent(record(0), record(1).toLong))
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
          .map(r => ClickEvent(r.requestId, r.clickTime))
          .saveToCassandra("tapsell", "clicks")
      })

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, Seconds(batchSeconds))
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()
  }
}
