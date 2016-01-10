import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtfilter.Plugin.FilterKeys._
import scoverage.ScoverageSbtPlugin._

object Build extends sbt.Build {

  lazy val chana = Project("chana", file("."))
    .aggregate(chana_avro)
    .dependsOn(chana_avro % "compile->compile;test->test")
    .settings(basicSettings: _*)
    .settings(RatsSettings.settings_chana: _*)
    .settings(Formatting.settings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(releaseSettings: _*)
    .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.rats)
    .settings(Packaging.settings: _*)
    .settings(instrumentSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(multiJvmSettings: _*)
    .settings(unmanagedSourceDirectories in Test += baseDirectory.value / "multi-jvm/scala")
    .configs(MultiJvm)

  lazy val chana_avro = Project("chana-avro", file("chana-avro"))
    .settings(basicSettings: _*)
    .settings(RatsSettings.settings_chana_avro: _*)
    .settings(Formatting.settings: _*)
    .settings(releaseSettings: _*)
    .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
    .settings(libraryDependencies ++= Dependencies.avro ++ Dependencies.jackson ++ Dependencies.test ++ Dependencies.rats)
    .settings(avroSettings: _*)
    .settings(instrumentSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val basicSettings = Seq(
    organization := "com.wandoulabs.chana",
    version := "0.2.0-SNAPSHOT",
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6", "-g"),
    resolvers ++= Seq(
      "Spray repo" at "http://repo.spray.io",
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
      "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven",
      "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"))

  // Todo rewrite sbt-avro to compile in Test phase.
  lazy val avroSettings = sbtavro.SbtAvro.avroSettings ++ Seq(
    sbtavro.SbtAvro.stringType in sbtavro.SbtAvro.avroConfig := "String",
    sourceDirectory in sbtavro.SbtAvro.avroConfig <<= (resourceDirectory in Test)(_ / "avsc"),
    javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceManaged in Test)(_ / "java" / "compiled_avro"),
    version in sbtavro.SbtAvro.avroConfig := "1.7.7",
    sourceGenerators in Test <+= (sbtavro.SbtAvro.generate in sbtavro.SbtAvro.avroConfig),
    managedSourceDirectories in Test <+= (javaSource in sbtavro.SbtAvro.avroConfig))

  lazy val releaseSettings = Seq(
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { (repo: MavenRepository) => false },
    pomExtra := pomXml)

  lazy val pomXml =
    (<url>https://github.com/wandoulabs/chana</url>
     <licenses>
       <license>
         <name>Apache License 2.0</name>
         <url>http://www.apache.org/licenses/</url>
         <distribution>repo</distribution>
       </license>
     </licenses>
     <scm>
       <url>git@github.com:wandoulabs/chana.git</url>
       <connection>scm:git:git@github.com:wandoulabs/chana.git</connection>
     </scm>
     <developers>
       <developer>
         <id>dcaoyuan</id>
         <name>Caoyuan DENG</name>
         <email>dcaoyuan@gmail.com</email>
       </developer>
       <developer>
         <id>cowboy129</id>
         <name>Xingrun CHEN</name>
         <email>cowboy129@gmail.com</email>
       </developer>
     </developers>)

  def multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    parallelExecution in Test := false,
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    })

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
    // https://github.com/sbt/sbt-pgp/issues/36
    publishTo := None)

}

object Dependencies {
  val SLF4J_VERSION = "1.7.7"
  val AKKA_VERSION = "2.4.1"
  val AKKA_STREAM_VERSION = "2.0.1"
  val SPRAY_VERSION = "1.3.3"

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-cluster-sharding" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-distributed-data-experimental" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-contrib" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-persistence" % AKKA_VERSION exclude ("org.iq80.leveldb", "leveldb"),
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AKKA_VERSION % Test,
    "org.iq80.leveldb" % "leveldb" % "0.7" % Runtime,
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Runtime,
    "com.github.krasserm" %% "akka-persistence-cassandra" % "0.3.6" % Runtime)

  val akka_http = Seq(
    "com.typesafe.akka" %% "akka-stream-experimental" % AKKA_STREAM_VERSION,
    "com.typesafe.akka" %% "akka-http-core-experimental" % AKKA_STREAM_VERSION,
    "com.typesafe.akka" %% "akka-http-experimental" % AKKA_STREAM_VERSION)

  val avro = Seq(
    "org.apache.avro" % "avro" % "1.7.7")

  val jackson = Seq("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3")

  val spray = Seq(
    "io.spray" %% "spray-can" % SPRAY_VERSION,
    "io.spray" %% "spray-routing-shapeless2" % SPRAY_VERSION,
    "io.spray" %% "spray-testkit" % SPRAY_VERSION % "test")

  val rats = Seq("xtc" % "rats-runtime" % "2.3.1")

  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.1.2")

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % Test,
    "org.scalatest" %% "scalatest" % "2.2.4" % "test")

  val basic: Seq[sbt.ModuleID] = akka ++ akka_http ++ avro ++ jackson ++ spray ++ log ++ test

  val all = basic
}

object Formatting {
  import com.typesafe.sbt.SbtScalariform
  import com.typesafe.sbt.SbtScalariform.ScalariformKeys
  import ScalariformKeys._

  val BuildConfig = config("build") extend Compile
  val BuildSbtConfig = config("buildsbt") extend Compile

  // invoke: build:scalariformFormat
  val buildFileSettings: Seq[Setting[_]] = SbtScalariform.noConfigScalariformSettings ++
    inConfig(BuildConfig)(SbtScalariform.configScalariformSettings) ++
    inConfig(BuildSbtConfig)(SbtScalariform.configScalariformSettings) ++ Seq(
      scalaSource in BuildConfig := baseDirectory.value / "project",
      scalaSource in BuildSbtConfig := baseDirectory.value,
      includeFilter in (BuildConfig, format) := ("*.scala": FileFilter),
      includeFilter in (BuildSbtConfig, format) := ("*.sbt": FileFilter),
      format in BuildConfig := {
        val x = (format in BuildSbtConfig).value
        (format in BuildConfig).value
      },
      ScalariformKeys.preferences in BuildConfig := formattingPreferences,
      ScalariformKeys.preferences in BuildSbtConfig := formattingPreferences)

  val settings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences)

  val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(IndentSpaces, 2)
  }
}

object Packaging {
  import com.typesafe.sbt.SbtNativePackager._
  import com.typesafe.sbt.packager.Keys._
  import com.typesafe.sbt.packager.archetypes._

  val settings = packagerSettings ++ deploymentSettings ++
    packageArchetype.java_application ++ Seq(
      mainClass in Compile := Some("chana.Chana"),
      name := "chana",
      NativePackagerKeys.packageName := "chana",
      bashScriptConfigLocation := Some("${app_home}/../conf/jvmopts"),
      bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf"""")
}

object RatsSettings {
  import SBTRatsPluginPatched._

  // Task defined for directly usage in sbt console
  // We have to define multiple tasks for each project, otherwise the context may by combined (why) 
  lazy val generateRatsSources_chana = taskKey[Seq[java.io.File]]("generate rats sources for chana")
  lazy val generateRatsSources_chana_avro = taskKey[Seq[java.io.File]]("generate rats sources for xpath")

  // Note you have to put rats file under scalaSource instead of resourceDirectory, since sbt-rats only checks:
  //   val inputFiles = (scalaSourceDir ** ("*.rats" | "*.syntax")).get.toSet 
  // when call FileFunction.cached
  lazy val settings_chana = sbtRatsSettings ++ Seq(
    ratsMainModule := Some((scalaSource in Compile).value / "chana" / "jpql" / "rats" / "JPQLGrammar.rats"),
    generateRatsSources_chana <<= runGenerators)
  lazy val settings_chana_avro = sbtRatsSettings ++ Seq(
    ratsMainModule := Some((scalaSource in Compile).value / "chana" / "xpath" / "rats" / "XPathGrammar.rats"),
    generateRatsSources_chana <<= runGenerators)
}
