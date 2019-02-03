import domain.{Click, ClickEvent}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Created by Ali on 2/3/2019.
  */
package object functions {
  def rddToRDDClick(input: RDD[(String, String)]) = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex({ (index, it) =>
      val or = offsetRanges(index)
      it.flatMap { kv =>
        val line = kv._2
        val record = line.replace("\n", "").split("\\t")
        val MS_IN_HOUR = 1000 * 60 * 60
        if (record.length == 7)
          Some(Click(record(1).toLong / MS_IN_HOUR * MS_IN_HOUR, record(0), record(1).toLong,
            Map("topic" -> or.topic, "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }
/*
  def rddToRDDImpression(input: RDD[(String, String)]) = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex({ (index, it) =>
      val or = offsetRanges(index)
      it.flatMap { kv =>
        val line = kv._2
        val record = line.split("\\t")
        if (record.length == 7)
          Some(Click(record(0), record(1).toLong,
            Map("topic" -> or.topic, "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }
*/
}
