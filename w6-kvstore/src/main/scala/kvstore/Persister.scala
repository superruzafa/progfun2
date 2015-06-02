package kvstore

import akka.actor._
import scala.concurrent.duration._
import scala.language.postfixOps
import kvstore.Persistence.Persisted
import kvstore.Persistence.Persist
import scala.concurrent.ExecutionContext.Implicits.global

object Persister {
  case class PersistFailed(key: String, id: Long)
  case class PersistComplete(key: String, id: Long)
  private case class PersistTimeout()
}

class Persister(persist: Persist, persistenceActor: ActorRef, timeoutPeriod: FiniteDuration = 1 seconds) extends Actor {
  import Persister._

  context.system.scheduler.scheduleOnce(timeoutPeriod) {
    self ! PersistTimeout
  }

  val retryer = context.actorOf(Props(new Retryer(persistenceActor, persist)))

  def receive: Actor.Receive = {
    case Persisted(key, id) =>
      context.stop(retryer)
      context.parent ! PersistComplete(key, id)
      context.become(dead)
    case PersistTimeout =>
      context.stop(retryer)
      context.parent ! PersistFailed(persist.key, persist.id)
      context.stop(self)
  }

  def dead: Receive = {
    case PersistTimeout => context.stop(self)
    case _ =>
  }
}