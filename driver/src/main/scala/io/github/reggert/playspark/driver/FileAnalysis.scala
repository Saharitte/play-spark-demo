package io.github.reggert.playspark.driver

import java.io.File

import org.apache.spark.{AccumulableParam, SparkEnv, SparkContext}
import SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.concurrent.{Promise, ExecutionContext, Future}

class FileAnalysis(sparkContext : SparkContext, sparkEnv : SparkEnv, files : Seq[File]) {
  val fileRDDs = files map {f => sparkContext.textFile(f.toString)}
  val parsedFileRDDs = fileRDDs map {f => f map {_.toDouble}}
  val doubles : RDD[Double] = sparkContext.union(parsedFileRDDs)

  import AsyncDoubleRDDFunctions._

  // This should go away once Spark gets rid of the ThreadLocal storage of SparkEnv.
  private def withSparkEnv[T] (f : => T) : T = {SparkEnv.set(sparkEnv); f}

  def stats()(implicit ctx : ExecutionContext) : Future[StatCounter] = withSparkEnv {
    val p = Promise[StatCounter]()
    p.completeWith(doubles.asyncStats)
    p.future
  }

  def histogram(bucketCount : Int)(implicit ctx : ExecutionContext) : Future[(Array[Double], Array[Long])] = withSparkEnv {
    val p = Promise[(Array[Double], Array[Long])]
    p.completeWith(doubles.asyncHistogram(bucketCount))
    p.future
  }
}
