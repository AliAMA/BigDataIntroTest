package config

import com.typesafe.config.ConfigFactory

/**
  * Created by Ali on 2/2/2019.
  */
object Settings {
  private val config = ConfigFactory.load()

  object ClickLogGenConf {
    private val clickLogGenConf = config.getConfig("clickstream")

    lazy val records = clickLogGenConf.getInt("records")
    lazy val timeMultiplier = clickLogGenConf.getInt("time_multiplier")
    lazy val filePath = clickLogGenConf.getString("file_path")
    lazy val destPath = clickLogGenConf.getString("dest_path")
    lazy val numberOfFiles = clickLogGenConf.getInt("number_of_files")
    lazy val kafkaTopic = clickLogGenConf.getString("kafka_topic")
    lazy val hdfsPath = clickLogGenConf.getString("hdfs_path")
  }

  object ImpressionLogGenConf {
    private val impressionLogGenConf = config.getConfig("impression")

    lazy val records = impressionLogGenConf.getInt("records")
    lazy val timeMultiplier = impressionLogGenConf.getInt("time_multiplier")
    lazy val applicationNameFile = impressionLogGenConf.getString("application_name_file_name")
    lazy val apps = impressionLogGenConf.getInt("apps")
    lazy val ads = impressionLogGenConf.getInt("ads")
    lazy val adsTitleFile = impressionLogGenConf.getString("ad_file_name")
    lazy val filePath = impressionLogGenConf.getString("file_path")
    lazy val destPath = impressionLogGenConf.getString("dest_path")
    lazy val numberOfFiles = impressionLogGenConf.getInt("number_of_files")
    lazy val kafkaTopic = impressionLogGenConf.getString("kafka_topic")
    lazy val hdfsPath = impressionLogGenConf.getString("hdfs_path")
  }

  object KafkaConf {
    private val kafkaConf = config.getConfig("kafka")

    lazy val bootstrapServers = kafkaConf.getString("bootstrap_servers")
    lazy val keySerializer = kafkaConf.getString("key_serializer")
    lazy val valueSerializer = kafkaConf.getString("value_serializer")
    lazy val acks = kafkaConf.getString("acks")
    lazy val clientIdPrefix = kafkaConf.getString("client_id")
  }

  object ClickStreamingJobConf extends Serializable {
    private val clickStreamingJobConf = config.getConfig("click_streaming_job")

    lazy val zookeeper = clickStreamingJobConf.getString("zookeeper")
    lazy val groupId = clickStreamingJobConf.getString("group_id")
    lazy val autoOffsetReset = clickStreamingJobConf.getString("auto_offset_reset")
    lazy val kafkaTopic = clickStreamingJobConf.getString("kafka_topic")
    lazy val keyspace = clickStreamingJobConf.getString("keyspace")
    lazy val table = clickStreamingJobConf.getString("table")
  }

  object ImpressionStreamingJobConf extends Serializable {
    private val impressionStreamingJobConf = config.getConfig("impression_streaming_job")

    lazy val zookeeper = impressionStreamingJobConf.getString("zookeeper")
    lazy val groupId = impressionStreamingJobConf.getString("group_id")
    lazy val autoOffsetReset = impressionStreamingJobConf.getString("auto_offset_reset")
    lazy val kafkaTopic = impressionStreamingJobConf.getString("kafka_topic")
    lazy val keyspace = impressionStreamingJobConf.getString("keyspace")
    lazy val table = impressionStreamingJobConf.getString("table")
  }

}
