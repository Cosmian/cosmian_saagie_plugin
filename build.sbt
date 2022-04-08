resolvers += Resolver.sbtPluginRepo("releases")

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "Cosmian",
  scalaVersion := "2.12.15",
  assembly / test := {},
  // add the local repository to enable quick development fixes
  resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)
lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("scala", xs @ _*)                   => MergeStrategy.discard
    case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
    case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
    case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
    case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
    case PathList("com", "google", xs @ _*)           => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
    case "about.html"                                 => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA"                      => MergeStrategy.last
    case "META-INF/mailcap"                           => MergeStrategy.last
    case "META-INF/mimetypes.default"                 => MergeStrategy.last
    case "plugin.properties"                          => MergeStrategy.last
    case "log4j.properties"                           => MergeStrategy.last
    case "META-INF/versions/11/module-info.class"     => MergeStrategy.last
    case "module-info.class"                          => MergeStrategy.last
    case "META-INF/io.netty.versions.properties"      => MergeStrategy.last
    case "META-INF/native-image/io.netty/common/native-image.properties" =>
      MergeStrategy.last
    case "META-INF/native-image/io.netty/transport/reflection-config.json" =>
      MergeStrategy.last
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)
lazy val dependencySettings = Seq(
  libraryDependencies ++= Seq(
    "com.cosmian" % "cosmian_java_lib" % "0.6.2",
    "com.cosmian" %% "cosmian_spark_crypto" % "1.0.0",
    "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
    "org.apache.hadoop" % "hadoop-common" % "3.3.1" % "provided",
    "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
    "org.scalatest" %% "scalatest" % "3.2.11" % "test"
  ),
  // Fixes a problem with the Jackson version used by downgrading it
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3" % "test",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3" % "test"
  )
)

lazy val decrypt = (project in file("."))
  .settings(
    name := "cosmian_spark"
    // assembly / mainClass := Some("com.cosmian.spark.Decrypt")
  )
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)
  .settings(dependencySettings: _*)
