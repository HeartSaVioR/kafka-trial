
name := "kafka-trial"

version := "0.1"

scalaVersion := "2.11.12"

val scalaMinorVersion = "2.11"
val kafkaVersion = "2.0.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += ("org.apache.kafka" % "kafka-streams" % kafkaVersion)
libraryDependencies += "org.json4s" %% s"json4s-jackson" % "3.5.3"
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts(
  Artifact("javax.ws.rs-api", "jar", "jar"))

// EmbeddedKafkaCluster
libraryDependencies ++= Seq(
  ("org.apache.kafka" %% s"kafka" % kafkaVersion) % "test",
  ("org.apache.kafka" %% s"kafka" % kafkaVersion classifier "test") % "test",
  ("org.apache.kafka" % "kafka-clients" % kafkaVersion classifier "test") % "test",
  ("org.apache.kafka" % "kafka-streams" % kafkaVersion classifier "test") % "test"
)

libraryDependencies ++= Seq(
  ("org.scalatest" %% "scalatest" % "3.0.3") % "test",
  ("junit" % "junit" % "4.12") % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

target in assembly := file("build")

assemblyJarName in assembly := s"${name.value}.jar"

javacOptions ++= Seq("-source", "1.8")