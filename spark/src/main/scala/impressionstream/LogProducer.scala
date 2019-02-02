package impressionstream

import java.util.Properties

import clickstream.LogProducer.{requests, rnd, _}
import config.Settings
import domain.ImpressionEvent
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Created by Ali on 2/3/2019.
  */
object LogProducer extends App {

  //Get the configs (Impressionstream Log & Kafka Config)
  val ilc = Settings.ImpressionLogGenConf
  val kc = Settings.KafkaConf

  //populating random data needed to create impressions
  val requests = (0 to ilc.records).map("Req-" + _)
  //Cause We don't have more than 12000 ad titles we don't allow ad config values with more than 12000
  val ads = ilc.ads match {
    case x if x > 12000 => (0 to 209000).map("Ad-" + _)
    case _ => (0 to ilc.ads).map("Ad-" + _)
  }
  val adTitles = scala.io.Source.fromInputStream(getClass.getResourceAsStream(ilc.adsTitleFile)).getLines().toArray

  //Cause We don't have more than ~209000 app titles we don't allow app config values with more than 209000
  val apps = ilc.apps match {
    case x if x > 209000 => (0 to 209000).map("App-" + _)
    case _ => (0 to ilc.apps).map("App-" + _)
  }

  val applicationNames = scala.io.Source.fromInputStream(getClass.getResourceAsStream(ilc.applicationNameFile)).getLines().toArray

  val rnd = new Random()

  val topic = ilc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kc.bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kc.keySerializer)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kc.valueSerializer)
  props.put(ProducerConfig.ACKS_CONFIG, kc.acks)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, kc.clientIdPrefix + "-ImpressionEvents")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val filePath = ilc.filePath
  val destPath = ilc.destPath

  for (fileCount <- 1 to ilc.numberOfFiles) {
    //Uncomment This Section to write clickstream logs in files
    //val fw = new FileWriter(filePath, true)

    // introduce some randomness to time increments
    val incrementTimeEvery = rnd.nextInt(ilc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    def createNewRndImpressionEvent = {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * ilc.timeMultiplier)
      timestamp = System.currentTimeMillis()

      val requestId = requests(rnd.nextInt(requests.length - 1))
      val adIdx = rnd.nextInt(ads.length - 1)
      val adId = ads(adIdx)
      val adTitle = adTitles(adIdx)
      val adCost: Double = rnd.nextDouble()
      val appIdx = rnd.nextInt(apps.length - 1)
      val appId = apps(appIdx)
      val appTitle = applicationNames(appIdx)
      val impressionTime = adjustedTimestamp

      s"$requestId\t$adId\t$adTitle\t$adCost\t$appId\t$appTitle\t$impressionTime\n"
    }

    for (iteration <- 1 to ilc.records) {
      val line: String = createNewRndImpressionEvent
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
