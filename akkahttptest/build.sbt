val akka_version = "2.4.19"
val akka_http_version = "10.0.10"
val aws_sdk_version = "2.0.0-preview-9"
val guava_version = "19.0"
val mockito_version = "2.8.9"
val slf4j_version = "1.6.4"
val scalatest_version = "3.0.4"
val scala_version = "2.12.3"

val group_id = "net.ndolgov"
val project_version = "1.0.0-SNAPSHOT"

val akkahttptest_aws_id = "akkahttptest-aws"
val akkahttptest_aws = Project(id = akkahttptest_aws_id, base = file(akkahttptest_aws_id)).
  settings(
    name         := akkahttptest_aws_id,
    organization := group_id,
    version      := project_version
  ).
  settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akka_version,
      "com.typesafe.akka" %% "akka-stream" % akka_version,
      "org.mockito" % "mockito-core" % mockito_version % Test,
      "org.scalatest" %% "scalatest" % scalatest_version % Test,
      "org.slf4j" % "slf4j-api" % slf4j_version,
      "org.slf4j" % "slf4j-log4j12" % slf4j_version,
      "software.amazon.awssdk" % "s3" % aws_sdk_version
    )
  )

val akkahttptest_web_id = "akkahttptest-web"
val akkahttptest_web = Project(id = akkahttptest_web_id, base = file(akkahttptest_web_id)).
  settings(
    name         := akkahttptest_web_id,
    organization := group_id,
    version      := project_version
  ).
  settings(
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % guava_version % Test, // ThreadFactoryBuilder
      "com.typesafe.akka" %% "akka-http" % akka_http_version,
      "com.typesafe.akka" %% "akka-http-core" % akka_http_version,
      "com.typesafe.akka" %% "akka-http-spray-json" % akka_http_version,
      "com.typesafe.akka" %% "akka-http-testkit" % akka_http_version % Test,
      "org.mockito" % "mockito-core" % mockito_version % Test,
      "org.scalatest" %% "scalatest" % scalatest_version % Test,
      "org.slf4j" % "slf4j-api" % slf4j_version,
      "org.slf4j" % "slf4j-log4j12" % slf4j_version,
      "software.amazon.awssdk" % "s3" % aws_sdk_version
    )
  )

val akkahttptest_root_id = "akkahttptest-root"
val root = Project(id = akkahttptest_root_id, base = file(".") ).
  settings(
    scalaVersion := scala_version,
    scalacOptions ++= Seq("-deprecation", "-Xfatal-warnings")
  ).
  settings(
    name         := akkahttptest_root_id,
    organization := group_id,
    version      := project_version
  ).
  settings(
    resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
  ).
  settings(
    packageBin := { new File("") },
    packageSrc := { new File("") }
  ).
  aggregate(akkahttptest_aws, akkahttptest_web)


