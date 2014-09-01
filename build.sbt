organization := "io.github.reggert"

name := "play-spark-demo"

version in ThisBuild := "0.0.1-SNAPSHOT"

scalaVersion in ThisBuild := "2.10.4"

val shared = project

val webapp = project.dependsOn(shared).enablePlugins(PlayScala)

val driver = project.dependsOn(shared)

val root = project.in(file(".")).aggregate(webapp, driver, shared)


