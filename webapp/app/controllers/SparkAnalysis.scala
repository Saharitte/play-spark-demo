package controllers

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import play.api.libs.concurrent.Akka
import play.api.libs.json.Json
import play.api.mvc.{Result, Action, Controller}
import akka.pattern.ask
import io.github.reggert.playspark.shared.FileAnalysisMessages._
import scala.concurrent.duration._

object SparkAnalysis extends Controller {

  implicit val statsWrites = Json.writes[Stats]
  implicit val timeout = Timeout(5 minutes)
  import play.api.Play.current
  import scala.concurrent.ExecutionContext.Implicits.global

  val config = ConfigFactory.load()

  val broker = Akka.system.actorSelection(config.getString("demo.spark-broker"))

  def statistics = Action.async {
    (broker ? RequestStats).mapTo[Stats] map {stats => Ok(Json.toJson(stats))}
  }
}
