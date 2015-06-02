package kvstore

import akka.actor.{ReceiveTimeout, Actor, ActorRef}
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.concurrent.duration._

class Retryer(val recipient: ActorRef, val messageToRetry: Any, val retryPeriod: Duration = 100 milliseconds) extends Actor {

  recipient ! messageToRetry
  context.setReceiveTimeout(retryPeriod)

  override def receive: Actor.Receive = {
    case ReceiveTimeout =>
      recipient ! messageToRetry
    case m => context.parent ! m
  }
}
