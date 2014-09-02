package io.github.reggert.playspark.shared

import scala.util.Random

object Generator extends App {

  def generate(count : Long, mean : Double, stdev : Double, seed : Option[Long] = None) : Iterator[Double] = {
    val rng = seed map {new Random(_)} getOrElse {new Random}
    var n = 0L // can't use take here because it only accepts Ints
    Iterator.continually {n += 1; rng.nextGaussian() * stdev + mean} takeWhile {_ => n <= count}
  }

  if (args.length < 3) {
    Console.err.println("Usage: Generator <count> <mean> <stdev> [<seed>] ")
    sys.exit(1)
  }

  val count = args(0).toLong
  val mean = args(1).toDouble
  val stdev = args(2).toDouble
  val seed = if (args.length >= 4) Some(args(3).toLong) else None

  generate(count, mean, stdev, seed) foreach println
}
