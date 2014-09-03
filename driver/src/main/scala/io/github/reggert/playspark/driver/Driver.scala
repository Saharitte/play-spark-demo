package io.github.reggert.playspark.driver

import java.io.File

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

object Driver extends App {
  val actorSystem = ActorSystem("io.github.reggert.playspark.driver.Driver")
  val sparkConf = new SparkConf()
  val files = args.toSeq map {new File(_)}
  val broker = actorSystem.actorOf(Props(new PlaySparkBroker(sparkConf, files)))
  actorSystem.awaitTermination()
}
