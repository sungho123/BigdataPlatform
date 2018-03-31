name := "wikipedia-language-ranking"

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
    "junit" % "junit" % "4.10" % Test,
    "org.scalatest" %% "scalatest" % "2.2.4",
    "org.apache.spark" %% "spark-core" % "2.2.0"
)

// for funsets
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
