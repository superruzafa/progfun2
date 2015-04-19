package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  // Supplied in assignment
  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  // If you insert any two elements into an empty heap,
  // finding the minimum of the resulting heap should get
  // the smallest of the two elements back.
  property("hint 1") = forAll { (x: Int, y: Int) =>
    findMin(insert(y, insert(x, empty))) == Math.min(x, y)
  }

  // If you insert an element into an empty heap,
  // then delete the minimum, the resulting heap should be empty.
  property("hint 2") = forAll { x: A =>
    isEmpty(deleteMin(insert(x, empty)))
  }

  // Given any heap, you should get a sorted sequence
  // of elements when continually finding and deleting minima.
  // (Hint: recursion and helper functions are your friends.)
  property("hint 3") = forAll { h: H =>
    def isSorted(h: H): Boolean =
      if (isEmpty(h)) true
      else {
        val h1 = deleteMin(h)
        isEmpty(h1) || (findMin(h) <= findMin(h1) && isSorted(h1))
      }
    isSorted(h)
  }

  // Finding a minimum of the melding of any two heaps
  // should return a minimum of one or the other.
  property("hint 4") = forAll { (h1: H, h2: H) =>
    def areEqual(h1: H, h2: H): Boolean =
      if (isEmpty(h1) && isEmpty(h2)) true
      else findMin(h1) == findMin(h2) && areEqual(deleteMin(h1), deleteMin(h2))
    areEqual(meld(h1, h2), meld(deleteMin(h1), insert(findMin(h1), h2)))
  }

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
