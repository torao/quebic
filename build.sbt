name := "quebic"

version := "1.0.1"

organization := "at.hazm"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.specs2" % "specs2-core_2.12" % "3.8.+" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.7.+"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

sonatypeProfileName := "at.hazm"

licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

homepage := Some(url("https://github.com/torao/quebic"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/torao/quebic"),
    "https://github.com/torao/quebic.git"
  )
)

developers := List(
  Developer(
    id    = "torao",
    name  = "TAKAMI Torao",
    email = "koiroha@gmail.com",
    url   = url("http://hazm.at")
  )
)
