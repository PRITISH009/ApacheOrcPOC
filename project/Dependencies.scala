import sbt._

object Dependencies {
  lazy val libs: List[ModuleID] = List(
    "org.apache.arrow.orc" % "arrow-orc" % "14.0.0",
    "org.apache.orc" % "orc-core" % "1.9.1",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",
    "com.typesafe" % "config" % "1.2.1"
  )
}
