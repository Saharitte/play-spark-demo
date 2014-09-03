package io.github.reggert.playspark.driver

import java.io.File

import akka.actor.{Status, ActorRef, Actor}
import akka.pattern.pipe
import io.github.reggert.playspark.shared.FileAnalysisMessages._
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

class PlaySparkBroker(val sparkConf : SparkConf, dataFiles : Seq[File]) extends SparkContextActor {
  import PlaySparkBroker._

  require(dataFiles.nonEmpty, "No files specified")

  private[this] var analysisHolder : Option[FileAnalysis] = None
  def analysis = analysisHolder.get
  def analysis_= (newAnalysis : FileAnalysis) : Unit = {analysisHolder = Some(newAnalysis)}
  def clearAnalysis() : Unit = {analysisHolder = None}


  override def preStart() : Unit = {
    dataFiles foreach {f => require(f.isFile, s"$f is not a file")}
    dataFiles foreach {f => require(f.canRead, s"$f is not readable")}
    super.preStart()
    analysis = new FileAnalysis(sparkContext, SparkEnv.get, dataFiles)
  }

  override def postStop() : Unit = {
    clearAnalysis()
    super.postStop()
  }

  override def receive: Receive = idle

  def idle : Receive = {
    case RequestStats =>
      import context.dispatcher
      analysis.stats() map StatsAnalysisComplete pipeTo self
      context become busy(Set(sender))
  }

  def busy(statsRequests : Set[ActorRef]) : Receive = {
    case RequestStats => context become busy(statsRequests + sender)
    case completed : StatsAnalysisComplete =>
      val stats = completed.stats
      statsRequests foreach {_ ! stats}
      context become idle
    case Status.Failure(cause) =>
      throw cause
  }
}


object PlaySparkBroker {

  private final case class StatsAnalysisComplete(statCounter : StatCounter) {
    def stats = Stats(
      count = statCounter.count,
      mean = statCounter.mean,
      variance = statCounter.variance,
      sampleVariance = statCounter.sampleVariance,
      stdev = statCounter.stdev,
      sampleStdev = statCounter.sampleStdev,
      sum = statCounter.sum,
      max = statCounter.max,
      min = statCounter.min
    )
  }

}
