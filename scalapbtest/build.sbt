import scalapb.compiler.Version.scalapbVersion

val grpc_version = "1.8.0"
val slf4j_version = "1.6.4"
val scalatest_version = "3.0.4"
val scala_version = "2.12.3"

val group_id = "net.ndolgov"
val project_version = "1.0.0-SNAPSHOT"

val scalapbtest_grpc_id = "scalapbtest-grpc"
val scalapbtest_grpc = Project(id = scalapbtest_grpc_id, base = file(scalapbtest_grpc_id)).
  settings(
    name         := scalapbtest_grpc_id,
    organization := group_id,
    version      := project_version
  ).
  settings(
    PB.protoSources in Compile := Seq(sourceDirectory.value / "main/proto"),
    PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value)
  ).
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatest_version % Test,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
      "io.grpc" % "grpc-netty" % grpc_version,
      "org.slf4j" % "slf4j-api" % slf4j_version,
      "org.slf4j" % "slf4j-log4j12" % slf4j_version,
    )
  )

val scalapbtest_root_id = "scalapbtest-root"
val root = Project(id = scalapbtest_root_id, base = file(".") ).
  settings(
    scalaVersion := scala_version,
    scalacOptions ++= Seq("-deprecation", "-Xfatal-warnings")
  ).
  settings(
    name         := scalapbtest_root_id,
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
  aggregate(scalapbtest_grpc)


