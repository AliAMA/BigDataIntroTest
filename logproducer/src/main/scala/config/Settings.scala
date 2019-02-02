package config

import com.typesafe.config.ConfigFactory

/**
  * Created by Ali on 1/31/2019.
  */
object Settings {

  private val config = ConfigFactory.load()

  object ClickLogGen {
      private val clickLogGen = config.getConfig("clickStream")

      lazy val records = clickLogGen.getInt("records")
      lazy val timeMultiplier = clickLogGen.getInt("time_multiplier")
      lazy val pages = clickLogGen.getInt("pages")
      lazy val visitors = clickLogGen.getInt("visitors")
      lazy val filePath  = clickLogGen.getInt("file_path")
  }
}
