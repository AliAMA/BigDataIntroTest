package clickstream

import java.io.FileWriter
import java.util.Properties

//import clickstream.LogProducer._
import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Created by Ali on 2/3/2019.
  */
object LogProducer extends App{
  //Get the configs (Clickstream Log & Kafka Config)
  val clc = Settings.ClickLogGenConf
  val kc = Settings.KafkaConf

  //Create clicks with random data
  val requests = (0 to clc.records).map("Req-" + _)

  val rnd = new Random()

  val topic = clc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kc.bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kc.keySerializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kc.valueSerializer)
  props.put(ProducerConfig.ACKS_CONFIG, kc.acks)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, kc.clientIdPrefix + "-ClickEvents")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val filePath = clc.filePath
  val destPath = clc.destPath

  for (fileCount <- 1 to clc.numberOfFiles) {
    //Uncomment This Section to write clickstream logs in files
    //val fw = new FileWriter(filePath, true)

    // introduce some randomness to time increments
    val incrementTimeEvery = rnd.nextInt(clc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to clc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * clc.timeMultiplier)
      timestamp = System.currentTimeMillis()

      val requestId = requests(rnd.nextInt(requests.length - 1))

      val line = s"$requestId\t$adjustedTimestamp\n"
      val producerRecord = new ProducerRecord(topic, line)
      kafkaProducer.send(producerRecord)

      //Uncomment This Section to write clickstream logs in files
      //fw.write(line)

      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }

    //Uncomment This Section to write clickstream logs in files
    /*
    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    */
    val sleeping = 2000
    println(s"Sleeping for $sleeping ms")

  }
}
