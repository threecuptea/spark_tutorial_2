name := "spark_tutorial_2"

version := "1.0"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8")
compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql"  % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib"  % "2.3.0" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector"  % "2.2.2" % "provided"
)
    