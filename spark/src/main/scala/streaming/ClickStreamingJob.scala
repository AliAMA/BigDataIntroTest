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
import org.apache.spark.sql.{SaveMode}
import _root_.kafka.serializer.StringDecoder
import domain.ClickEvent
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.WriteConf

import scala.util.Try

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

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> csc.zookeeper,
        "group.id" -> csc.groupId,
        "auto.offset.reset" -> csc.autoOffsetReset)

      var fromOffsets: Map[TopicAndPartition, Long] = Map.empty
      val hdfsPath = csc.hdfsPath
      val topic = csc.kafkaTopic

      Try(sqlContext.read.parquet(hdfsPath)).foreach(hdfsData =>
        fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
          .collect().map { row =>
          (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
        }.toMap
      )
      println("@254: oheub")
      val kafkaDirectStream = fromOffsets.isEmpty match {
        case true =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaDirectParams, Set(topic)
          )
        case false =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParams, fromOffsets, { mmd: MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
          )
      }

      val activityStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDClick(input)
      }).cache()

      // save data to HDFS
      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "requestId", "clickTime", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")

        activityDF
          .write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }

      val stream = kafkaDirectStream.transform { input =>
        val inputRDD = input.flatMap { line =>
          val record = line._2.replace("\n", "").split("\\t")
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

        inputRDD.saveToCassandra(csc.keyspace, csc.table)
        inputRDD
      }.cache()

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, Seconds(batchSeconds))
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()
  }
}
