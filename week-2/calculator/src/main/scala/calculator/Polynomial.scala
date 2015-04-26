package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal {
      Math.pow(b(), 2) - 4 * a() * c()
    }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal {
      val aValue: Double = a()
      val bValue: Double = b()
      delta() match {
        case d if d < 0.0 => Set()
        case 0.0 => Set(computeRoot(aValue, bValue, 0))
        case d =>
          val dSqrt: Double = Math sqrt d
          Set(computeRoot(aValue, bValue, dSqrt), computeRoot(aValue, bValue, -dSqrt))
      }
    }
  }

  private def computeRoot(a: Double, b: Double, dSqrt: Double): Double = {
    (-b + dSqrt) / (2 * a)
  }
}
