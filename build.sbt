name := "scala_twitter"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.4"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.12" % "2.4.0"