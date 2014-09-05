name := "play-spark-demo-driver"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2"

fork in run := true

javaOptions in run ++= Seq("-Xmx512M", "-Dspark.master=local[*]", "-Dspark.app.name=PlaySparkDemo")
