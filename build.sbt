name := "quebic"

version := "1.0.0"

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

pomExtra :=
  <url>https://github.com/torao/quebic</url>
    <licenses>
      <license>
        <name>Apache-2.0</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0</url>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/torao/quebic</url>
      <connection>https://github.com/torao/quebic.git</connection>
    </scm>
    <developers>
      <developer>
        <id>torao</id>
        <name>TAKAMI Toral</name>
        <url>http://hazm.at</url>
      </developer>
    </developers>