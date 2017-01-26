name := "spark_tutorial_2"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql"  % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-mllib"  % "2.1.0" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector"  % "2.0.0" % "provided"

)
    