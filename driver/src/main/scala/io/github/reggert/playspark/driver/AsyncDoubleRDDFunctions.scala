package io.github.reggert.playspark.driver

import org.apache.spark.{AccumulatorParam, FutureAction, AccumulableParam, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import SparkContext._

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Helper class that merges the functionality of [[org.apache.spark.rdd.DoubleRDDFunctions]]
 * and [[org.apache.spark.rdd.AsyncRDDActions]]
 *
 * @param self the RDD to extend with the additional functionality
 */
final class AsyncDoubleRDDFunctions(val self : RDD[Double]) extends AnyVal {
  import AsyncDoubleRDDFunctions._


  def asyncStats()(implicit ctx : ExecutionContext) : Future[StatCounter] = {
    val p = Promise[StatCounter]()
    val acc = self.sparkContext.accumulable[StatCounter, Double](new StatCounter) (accumulableStats)
    p.completeWith(self foreachAsync {acc += _} map {_ => acc.value})
    p.future
  }


  def asyncHistogram(bucketCount: Int) (implicit ctx : ExecutionContext) : Future[Pair[Array[Double], Array[Long]]] = {
    // Compute the minimum and the maximum
    val mappedPartitions: RDD[MinMax] = self.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e))))
    } map {case (max, min) => MinMax(min, max)}
    implicit val acc =
      self.sparkContext.accumulator(MinMax(Double.PositiveInfinity, Double.NegativeInfinity)) (accumulatorMinMax)
    val minMaxFuture = mappedPartitions foreachAsync {acc ++= _} map {_ => acc.value} map {
      case minMax : MinMax if minMax.unbounded =>
        throw new UnsupportedOperationException(
          "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
      case minMax => minMax
    }
    for {
      MinMax(min, max) <- minMaxFuture
      increment = (max-min)/bucketCount.toDouble
      range = if (increment != 0) {
        Range.Double.inclusive(min, max, increment)
      } else {
        List(min, min)
      }
      buckets = range.toArray
      histogram <- asyncHistogram(buckets, true)
    } yield (buckets, histogram)
  }


  def asyncHistogram(buckets : Array[Double], evenBuckets : Boolean)(implicit ctx : ExecutionContext) : Future[Array[Long]] = {
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // The histogramPartition function computes the partial histogram for a given
    // partition. The provided bucketFunction determines which bucket in the array
    // to increment or returns None if there is no bucket. This is done so we can
    // specialize for uniformly distributed buckets and save the O(log n) binary
    // search cost.
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
    Iterator[Array[Long]] = {
      val counters = new Array[Long](buckets.length - 1)
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => {counters(x) += 1}
          case _ => {}
        }
      }
      Iterator(counters)
    }
    // Merge the counters.
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }
    // Basic bucket function. This works using Java's built in Array
    // binary search. Takes log(size(buckets))
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e)
      if (location < 0) {
        // If the location is less than 0 then the insertion point in the array
        // to keep it sorted is -location-1
        val insertionPoint = -location-1
        // If we have to insert before the first element or after the last one
        // its out of bounds.
        // We do this rather than buckets.lengthCompare(insertionPoint)
        // because Array[Double] fails to override it (for now).
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {
        // Exact match, just insert here
        Some(location)
      } else {
        // Exact match to the last element
        Some(location - 1)
      }
    }
    // Determine the bucket function in constant time. Requires that buckets are evenly spaced
    def fastBucketFunction(min: Double, increment: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      if (e.isNaN()) {
        return None
      }
      val bucketNumber = (e - min)/(increment)
      // We do this rather than buckets.lengthCompare(bucketNumber)
      // because Array[Double] fails to override it (for now).
      if (bucketNumber > count || bucketNumber < 0) {
        None
      } else {
        Some(bucketNumber.toInt.min(count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decission need only be made
    // once rather than once per shard
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets(0), buckets(1)-buckets(0), buckets.length-1) _
    } else {
      basicBucketFunction _
    }
    val mappedPartitions : RDD[Array[Long]] = self.mapPartitions(histogramPartition(bucketFunction))

    val acc = self.sparkContext.accumulator(new Array[Long](buckets.length - 1))(new AccumulatorParam[Array[Long]]{
      override def addInPlace(r1: Array[Long], r2: Array[Long]): Array[Long] = mergeCounters(r1, r2)
      override def zero(initialValue: Array[Long]): Array[Long] = initialValue.clone()
    })
    mappedPartitions foreachAsync {acc ++= _} map {_ => acc.value}
  }

}


object AsyncDoubleRDDFunctions {
  implicit def rddToAsyncDoubleRDDFunctions(self : RDD[Double]) : AsyncDoubleRDDFunctions =
    new AsyncDoubleRDDFunctions(self)

  implicit val accumulableStats = new AccumulableParam[StatCounter, Double] {
    override def addAccumulator(r: StatCounter, t: Double): StatCounter = r.merge(t)
    override def addInPlace(r1: StatCounter, r2: StatCounter): StatCounter = r1.merge(r2)
    override def zero(initialValue: StatCounter): StatCounter = initialValue.copy()
  }

  final case class MinMax(min : Double, max : Double) {
    def unbounded = min.isNaN || max.isNaN | min.isInfinity || max.isInfinity
  }

  implicit val accumulatorMinMax = new AccumulatorParam[MinMax] {
    override def addInPlace(r1: MinMax, r2: MinMax): MinMax = MinMax(r1.min.min(r2.min), r1.max.max(r2.max))
    override def zero(initialValue: MinMax): MinMax = initialValue
  }
}
