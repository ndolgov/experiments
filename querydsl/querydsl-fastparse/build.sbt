val fastparse_version = "1.0.0"
val scalatest_version = "3.0.4"
val scala_version = "2.12.3"

val querydsl_fastparse_id = "querydsl-fastparse"

lazy val root = Project(id = querydsl_fastparse_id, base = file(".") ).
  settings(
    scalaVersion := scala_version,
    scalacOptions ++= Seq("-deprecation", "-Xfatal-warnings")
  ).
  settings(
    name         := querydsl_fastparse_id,
    organization := "net.ndolgov.querydsl",
    version      := "1.0.0-SNAPSHOT"
  ).
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatest_version % Test,
      "com.lihaoyi" %% "fastparse" % fastparse_version,
      "net.ndolgov.querydsl" % "querydsl-dsl" % "1.0.0-SNAPSHOT"
    )
  ).
  settings(
    resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
  )


