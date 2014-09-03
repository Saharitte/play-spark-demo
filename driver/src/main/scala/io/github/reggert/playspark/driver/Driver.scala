package io.github.reggert.playspark.driver

import java.io.File

import akka.actor.{Props, ActorSystem}
import org.apache.spark.SparkConf

object Driver extends App {
  val actorSystem = ActorSystem("PlaySparkDemo")
  val sparkConf = new SparkConf()
  val files = args.toSeq map {new File(_)}
  val broker = actorSystem.actorOf(Props(new PlaySparkBroker(sparkConf, files)), "PlaySparkBroker")
  actorSystem.awaitTermination()
}
