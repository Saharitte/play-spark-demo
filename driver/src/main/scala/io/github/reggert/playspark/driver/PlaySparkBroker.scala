package io.github.reggert.playspark.driver

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.apache.spark.{SparkConf, SparkContext}

class PlaySparkBroker(val sparkConf : SparkConf) extends SparkContextActor {
  override def receive: Receive = ???
}


object PlaySparkBroker {

}
