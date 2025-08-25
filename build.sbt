ThisBuild / scalaVersion := "3.3.4"
lazy val pekkoV = "1.1.5"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.pekko" %% "pekko-actor-typed"           % pekkoV,
    "org.apache.pekko" %% "pekko-cluster-typed"         % pekkoV,
    "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoV,
    "ch.qos.logback"    % "logback-classic"             % "1.5.18"
  )
)

lazy val limiterCore = (project in file("modules/limiter-core"))
  .settings(commonSettings)
  .settings(name := "limiter-core")

lazy val limiterSim = (project in file("modules/limiter-simulation"))
  .dependsOn(limiterCore)
  .settings(commonSettings)
  .settings(name := "limiter-simulation", fork := true, Compile / mainClass := Some("com.example.Driver"))
