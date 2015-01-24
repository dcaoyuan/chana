import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtfilter.Plugin.FilterKeys._
import scoverage.ScoverageSbtPlugin._

object Build extends sbt.Build {

  lazy val avpath = Project("wandou-astore", file("."))
    .settings(basicSettings: _*)
    .settings(Formatting.settings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(releaseSettings: _*)
    .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
    .settings(libraryDependencies ++= Dependencies.basic)
    .settings(Packaging.settings: _*)
    .settings(instrumentSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(SbtMultiJvm.multiJvmSettings ++ multiJvmSettings: _*)
    .settings(unmanagedSourceDirectories in Test += baseDirectory.value / "multi-jvm/scala")
    .configs(MultiJvm)

  lazy val basicSettings = Seq(
    organization := "com.wandoulabs.avro",
    version := "0.1.2-SNAPSHOT",
    scalaVersion := "2.11.5",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    resolvers ++= Seq(
      "Spray repo" at "http://repo.spray.io",
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"))

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
    (<url>https://github.com/wandoulabs/astore</url>
     <licenses>
       <license>
         <name>Apache License 2.0</name>
         <url>http://www.apache.org/licenses/</url>
         <distribution>repo</distribution>
       </license>
     </licenses>
     <scm>
       <url>git@github.com:wandoulabs/astore.git</url>
       <connection>scm:git:git@github.com:wandoulabs/astore.git</connection>
     </scm>
      <developers>
        <developer>
          <id>dcaoyuan</id>
          <name>Caoyuan DENG</name>
          <email>dcaoyuan@gmail.com</email>
        </developer>
      </developers>)

  def multiJvmSettings = Seq(
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
  val AKKA_VERSION = "2.3.9"
  val AKKA_STREAM_VERSION = "1.0-M2"
  val SPRAY_VERSION = "1.3.2"

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-contrib" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-persistence-experimental" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % "test",
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AKKA_VERSION % "test")

  val akka_http = Seq(
    "com.typesafe.akka" %% "akka-stream-experimental" % AKKA_STREAM_VERSION,
    "com.typesafe.akka" %% "akka-http-experimental" % AKKA_STREAM_VERSION,
    "com.typesafe.akka" %% "akka-http-core-experimental" % AKKA_STREAM_VERSION,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % AKKA_STREAM_VERSION
  )

  val avro = Seq(
    "org.apache.avro" % "avro" % "1.7.7")

  val avpath = Seq(
    "com.wandoulabs.avro" %% "wandou-avpath" % "0.1.3-SNAPSHOT")

  val spray = Seq(
    "io.spray" %% "spray-can" % SPRAY_VERSION,
    "io.spray" %% "spray-routing-shapeless2" % SPRAY_VERSION,
    "io.spray" %% "spray-testkit" % SPRAY_VERSION % "test")

  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.1.2")

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2-RC1" % "test",
    "org.scalatest" %% "scalatest" % "2.1.3" % "test")

  val basic: Seq[sbt.ModuleID] = akka ++ akka_http ++ avro ++ avpath ++ spray ++ log ++ test

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
      mainClass in Compile := Some("wandou.astore.AStore"),
      name := "astore",
      NativePackagerKeys.packageName := "astore",
      bashScriptConfigLocation := Some("${app_home}/../conf/jvmopts"),
      bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf"""")
}
