/*
 * Copyright 2018 Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Dependencies._
import xerial.sbt.Sonatype._

lazy val root = (project in file(".")).
 settings(
   inThisBuild(List(
     version      := "0.1.2"
     // mainClass in Compile := Some("com.sparkfits.examples.OnionSpace")
   )),
   // Name of the application
   name := "spark3D",
   // Name of the orga
   organization := "com.github.astrolabsoftware",
   // Do not execute test in parallel
   parallelExecution in Test := false,
   // Fail the test suite if statement coverage is < 70%
   coverageFailOnMinimum := true,
   coverageMinimum := 70,
   // Put nice colors on the coverage report
   coverageHighlighting := true,
   // Do not publish artifact in test
   publishArtifact in Test := false,
   // Exclude runner class for the coverage
   coverageExcludedPackages := "<empty>;com.spark3d.examples*",
   // Excluding Scala library JARs that are included in the binary Scala distribution
   // assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
   // Shading to avoid conflicts with pre-installed nom.tam.fits library
   // Uncomment if you have such conflicts.
   // assemblyShadeRules in assembly := Seq(ShadeRule.rename("nom.**" -> "new_nom.@1").inAll),
   // Put dependencies of the library
   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
     "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
     // For loading FITS files
     "com.github.JulienPeloton" %% "spark-fits" % "0.4.0",
     // "org.datasyslab" % "geospark" % "1.1.3",
     // Uncomment if you want to trigger visualisation
     // "com.github.haifengl" % "smile-plot" % "1.5.1",
     // "com.github.haifengl" % "smile-core" % "1.5.1",
     // "com.github.haifengl" % "smile-math" % "1.5.1",
     // "com.github.haifengl" %% "smile-scala" % "1.5.1",
     scalaTest % Test
   )
 )

// POM settings for Sonatype
homepage := Some(
 url("https://github.com/astrolabsoftware/spark3D")
)
scmInfo := Some(
 ScmInfo(
   url("https://github.com/astrolabsoftware/spark3D"),
   " https://github.com/astrolabsoftware/spark3D.git"
 )
)

developers := List(
 Developer(
   "JulienPeloton",
   "Julien Peloton",
   "peloton@lal.in2p3.fr",
   url("https://github.com/JulienPeloton")
 ),
 Developer(
 "ChristianArnault",
 "Christian Arnault",
 "xx@yy.zz",
 url("https://github.com/ChristianArnault")
 ),
 Developer(
 "mayurdb",
 "Mayur Bhosale",
 "mayurdb31@gmail.com",
 url("https://github.com/mayurdb")
 )
)

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

publishMavenStyle := true

publishTo := {
 val nexus = "https://oss.sonatype.org/"
 if (isSnapshot.value)
  Some("snapshots" at nexus + "content/repositories/snapshots")
 else
  Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
