package io.github.reggert.playspark.shared

object FileAnalysisMessages {
  case object RequestStats

  final case class Stats(
      count : Long,
      mean : Double,
      variance : Double,
      sampleVariance : Double,
      stdev : Double,
      sampleStdev : Double,
      sum : Double,
      max : Double,
      min : Double
    )
}
