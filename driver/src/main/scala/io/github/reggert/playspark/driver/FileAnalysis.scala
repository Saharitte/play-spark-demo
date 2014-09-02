package io.github.reggert.playspark.driver

import java.io.File

import org.apache.spark.{AccumulableParam, SparkEnv, SparkContext}
import SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.concurrent.{Promise, ExecutionContext, Future}

class FileAnalysis(sparkContext : SparkContext, sparkEnv : SparkEnv, files : Seq[File]) {
  val fileRDDs = files map {f => sparkContext.textFile(f.toString)}
  val doubles : RDD[Double] = for {
    f <- fileRDDs
    line <- f
  } yield line.toDouble

  import AsyncDoubleRDDFunctions._

  def stats : Future[StatCounter] = {
    val p = Promise[StatCounter]()
    p.completeWith(doubles.asyncStats)
    p.future
  }

  def histogram(bucketCount : Int) : Future[(Array[Double], Array[Long])] = {
    val p = Promise[(Array[Double], Array[Long])]
    p.completeWith(doubles.histogram(bucketCount))
    p.future
  }
}
