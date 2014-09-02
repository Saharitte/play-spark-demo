package io.github.reggert.playspark.driver

import java.io.File

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.apache.spark.{SparkEnv, SparkConf, SparkContext}

class PlaySparkBroker(val sparkConf : SparkConf, dataFiles : Seq[File]) extends SparkContextActor {

  private[this] var analysisHolder : Option[FileAnalysis] = None
  def analysis = analysisHolder.get
  def analysis_= (newAnalysis : FileAnalysis) : Unit = {analysisHolder = Some(newAnalysis)}
  def clearAnalysis() : Unit = {analysisHolder = None}


  override def preStart() : Unit = {
    super.preStart()
    analysis = new FileAnalysis(sparkContext, SparkEnv.get, dataFiles)
  }

  override def postStop() : Unit = {
    clearAnalysis()
    super.postStop()
  }

  override def receive: Receive = ???
}


object PlaySparkBroker {

}
