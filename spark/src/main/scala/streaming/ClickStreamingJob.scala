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
    //Getting click streaming config
    val csc = Settings.ClickStreamingJobConf
    // setup spark context
    val sc = getSparkContext("Streaming with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchSeconds = 4

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val kafkaParams = Map(
        "zookeeper.connect" -> csc.zookeeper,
        "group.id" -> csc.groupId,
        "auto.offset.reset" -> csc.autoOffsetReset)

      val kstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Map(csc.kafkaTopic -> 1), StorageLevel.MEMORY_AND_DISK)
        .map(_._2)

      val stream = kstream.transform { input =>
        val inputRDD = input.flatMap { line =>
          val record = line.replace("\n", "").split("\\t")
          println("@254: " + line)
          if (record.length == 2)
          // make sure we have complete records
            Some(ClickEvent(record(0), record(1).toLong))
          else
            None
        }

        //val df = inputRDD.toDF()
        //df.registerTempTable("clicks")
        //sqlContext.cacheTable("clicks")
        //Ready To Query Against Cached Temp Table

        inputRDD
      }.cache()

      stream.foreachRDD(rdd => {
        rdd
          //.map(r => ClickEvent(r.requestId, r.clickTime))
          .saveToCassandra(csc.keyspace, csc.table)
      })

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, Seconds(batchSeconds))
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()
  }
}
