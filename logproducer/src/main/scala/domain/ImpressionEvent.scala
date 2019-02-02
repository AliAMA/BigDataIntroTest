package domain

/**
  * Created by Ali on 1/31/2019.
  */
class ImpressionEvent {
  val requestId: String,
  val adId: String,
  val adTitle: String,
  val advertiserCost: Double,
  val appId: String,
  val appTitle: String,
  val impressionTime: Long
}
