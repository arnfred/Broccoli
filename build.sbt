import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "broccoli"

scalaSource in Compile <<= baseDirectory(_ / "src")

scalacOptions ++= Seq("-unchecked", "-Ywarn-dead-code", "-deprecation")

libraryDependencies  ++= Seq(
            // other dependencies here
            // pick and choose:
            "org.scalacheck" %% "scalacheck" % "1.11.5",
	    "org.scalatest" %% "scalatest" % "2.2.1",
            "joda-time" % "joda-time" % "2.2",
            "org.joda" % "joda-convert" % "1.2"
)

resolvers ++= Seq(
            // other resolvers here
            // if you want to use snapshot builds (currently 0.2-SNAPSHOT), use this.
			"akr4 release" at "http://akr4.github.com/mvn-repo/releases",
			"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
			"Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
			"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories snapshots"
)

// The main class
mainClass in (Compile, run) := Some("broccoli.Main")
