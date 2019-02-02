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
    lazy val apps = impressionLogGenConf.getInt("apps")
    lazy val filePath = impressionLogGenConf.getString("file_path")
    lazy val destPath = impressionLogGenConf.getString("dest_path")
    lazy val numberOfFiles = impressionLogGenConf.getInt("number_of_files")
    lazy val kafkaTopic = impressionLogGenConf.getString("kafka_topic")
    lazy val hdfsPath = impressionLogGenConf.getString("hdfs_path")
  }

}
