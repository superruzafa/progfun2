/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC =>
      val tempRoot = createRoot
      context.become(garbageCollecting(tempRoot))
      root ! CopyTo(tempRoot)
    case msg: Operation => root forward msg
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case msg: Operation => pendingQueue = pendingQueue :+ msg
    case GC =>
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      pendingQueue foreach {newRoot ! _}
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal
  
  def position(nElement: Int) = if (nElement > elem) Right else Left
  
  def internalOp(nElement: Int, requestor: ActorRef, msg: Operation, elementFound: => Unit, elementNotFound: => Unit) =
      if (nElement == elem) {
        elementFound
        requestor ! OperationFinished(msg.id)
      } else {
        val position = if (nElement > elem) Right else Left
        if (subtrees.contains(position))
          subtrees(position) forward msg
        else {
          elementNotFound
          requestor ! OperationFinished(msg.id)
        }
      }      

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg @ Insert(requestor, id, nElement) =>
      internalOp(nElement, requestor, msg, { removed = false },
                 {subtrees = subtrees + (position(nElement) -> context.actorOf(BinaryTreeNode.props(nElement, false)))}
      )

    case msg @ Remove(requestor, id, nElement) =>
      internalOp(nElement, requestor, msg, { removed = true }, {})

    case msg @ Contains(requestor, id, nElement) =>
      if (nElement == elem)
        requestor ! ContainsResult(id, !removed)
      else {
        val position = if (nElement > elem) Right else Left
        if (subtrees.contains(position))
          subtrees(position) forward msg
        else
          requestor ! ContainsResult(id, result = false)
      }

    case CopyTo(newTree) =>
      val children = subtrees.values.toSet
      if (children.isEmpty && removed)
        context.parent ! CopyFinished
      else {
        children.foreach { x => x ! CopyTo(newTree)}
        if (!removed) newTree ! Insert(self, elem, elem)
        context.become(copying(children, removed), discardOld = false)
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) =>
      if (expected.isEmpty)
        context.parent ! CopyFinished
      else
        context.become(copying(expected, insertConfirmed = true), discardOld = false)

    case CopyFinished =>
      val todo = expected - sender
      if (todo.isEmpty && insertConfirmed)
        context.parent ! CopyFinished
      else
        context.become(copying(todo, insertConfirmed), discardOld = false)
  }
}
