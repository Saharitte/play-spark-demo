package controllers

import akka.util.Timeout
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

  val broker = Akka.system.actorSelection("akka.tcp://PlayScalaDemo@127.0.0.1:2552/user/PlaySparkBroker")

  def statistics = Action.async {
    (broker ? RequestStats).mapTo[Stats] map {stats => Ok(Json.toJson(stats))}
  }
}
