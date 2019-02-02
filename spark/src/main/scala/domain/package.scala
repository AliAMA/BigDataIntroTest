/**
  * Created by Ali on 2/2/2019.
  */
package object domain {

  case class ImpressionEvent(requestId: String,
                             adId: String,
                             adTitle: String,
                             advertiserCost: Double,
                             appId: String,
                             appTitle: String,
                             impressionTime: Long
                            )

  case class ClickEvent(requestId: String,
                        clickTime: Long
                       )

}
