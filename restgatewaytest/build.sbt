//val grpc_version = "1.8.0"
val http_client_version = "4.1.3"
val jackson_version = "2.9.2"
val slf4j_version = "1.6.4"
val scalapb_gateway_version = "0.0.8"
val scalatest_version = "3.0.4"
val scala_version = "2.12.3"

val group_id = "net.ndolgov"
val project_version = "1.0.0-SNAPSHOT"

val restgatewaytest_web_id = "restgatewaytest-web"
val restgatewaytest_web = Project(id = restgatewaytest_web_id, base = file(restgatewaytest_web_id)).
  settings(
    name         := restgatewaytest_web_id,
    organization := group_id,
    version      := project_version
  ).
  settings(
    PB.protoSources in Compile := Seq(sourceDirectory.value / "main/proto"),
    PB.targets in Compile := Seq(
      // compile your proto files into scala source files
      scalapb.gen() -> (sourceManaged in Compile).value,
      // generate Swagger spec files into the `resources/specs`
      grpcgateway.generators.SwaggerGenerator -> (resourceDirectory in Compile).value / "specs",
      // generate the Rest Gateway source code
      grpcgateway.generators.GatewayGenerator -> (sourceManaged in Compile).value
    )
//    PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value)
  ).
  settings(
    resolvers += Resolver.bintrayRepo("beyondthelines", "maven")
  ).
  settings(
    libraryDependencies ++= Seq(
      "beyondthelines" %% "grpcgatewayruntime" % scalapb_gateway_version % "compile,protobuf",
      "com.fasterxml.jackson.core" % "jackson-databind" % jackson_version % Test,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson_version % Test,
      "org.apache.httpcomponents" % "httpasyncclient" % http_client_version % Test,
      "org.apache.httpcomponents" % "httpcore" % http_client_version % Test,
      "org.scalatest" %% "scalatest" % scalatest_version % Test,
      "org.slf4j" % "slf4j-api" % slf4j_version,
      "org.slf4j" % "slf4j-log4j12" % slf4j_version
    )
  )

val restgatewaytest_root_id = "restgatewaytest-root"
val root = Project(id = restgatewaytest_root_id, base = file(".") ).
  settings(
    scalaVersion := scala_version,
    scalacOptions ++= Seq("-deprecation", "-Xfatal-warnings")
  ).
  settings(
    name         := restgatewaytest_root_id,
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
  aggregate(restgatewaytest_web)


