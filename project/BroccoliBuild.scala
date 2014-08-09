import sbt._
import sbt.Keys._

object BroccoliBuild extends Build {

  lazy val broccoli = Project(
    id = "broccoli",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "Broccoli",
      organization := "org.dynkarken",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.11.2"
      // add other settings here
    )
  )
}
