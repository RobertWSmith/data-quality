lazy val sparkVersion  = "2.4.2"
lazy val hadoopVersion = "2.6.0"
lazy val hiveVersion   = "1.1.0"

lazy val commonSettings = Seq(
  organization := "com.github.robertwsmith",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.11.12",
  Test / fork := true,
  Test / parallelExecution := false,
  Test / javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSCClassUnloadingEnabled"),
  libraryDependencies ++= Seq(
    "org.scala-lang"    % "scala-reflect"     % scalaVersion.value,
    "org.apache.spark" %% "spark-core"        % sparkVersion  % Provided,
    "org.apache.spark" %% "spark-hive"        % sparkVersion  % Provided,
    "org.apache.spark" %% "spark-mllib"       % sparkVersion  % Provided,
    "org.apache.spark" %% "spark-mllib-local" % sparkVersion  % Provided,
    "org.apache.spark" %% "spark-sql"         % sparkVersion  % Provided,
    "ml.dmlc"           % "xgboost4j-spark"   % "0.82"        % Provided,
    "org.apache.hadoop" % "hadoop-common"     % hadoopVersion % Provided,
    "org.apache.hive"   % "hive-metastore"    % hiveVersion   % Provided
  ),
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-explaintypes",
    "-Yno-adapted-args"
  )
)

lazy val root = (project in file("."))
  .settings(name := "data-quality", commonSettings)
