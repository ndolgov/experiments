addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.13")

resolvers += Resolver.bintrayRepo("beyondthelines", "maven")

val scalapb_plugin_version = "0.6.7"
val scalapb_gateway_version = "0.0.8"

libraryDependencies ++= Seq(
  "com.trueaccord.scalapb" %% "compilerplugin" % scalapb_plugin_version,
  "beyondthelines" %% "grpcgatewaygenerator" % scalapb_gateway_version
)