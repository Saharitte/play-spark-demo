package io.github.reggert.playspark.driver

import akka.actor.Actor
import org.apache.spark.{SparkContext, SparkConf}

trait SparkContextActor extends Actor {
  def sparkConf : SparkConf

  private[this] var sparkContextHolder : Option[SparkContext] = None
  def sparkContext = sparkContextHolder.get
  private def sparkContext_= (newSparkContext : SparkContext) : Unit = {sparkContextHolder = Some(newSparkContext)}
  private def clearSparkContext() : Unit = {sparkContextHolder = None}

  override def preStart() : Unit = {
    sparkContext = new SparkContext(sparkConf)
  }

  override def postStop() : Unit = {
    sparkContext.stop()
    clearSparkContext()
  }
}
