/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version
import xerial.sbt.pack.PackPlugin._

val scioVersion = "0.4.4"
val beamVersion = "2.1.0"
val slf4jVersion = "1.7.25"
val autoValueVersion = "1.4.1"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Sonatype.sonatypeSettings ++ Seq(
  organization := "com.spotify",
  scalaVersion := "2.12.4",
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile, doc) += "-no-java-comments",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc)  := Seq("-source", "1.8"),

  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",

  // Repositories and dependencies
  resolvers += Resolver.sonatypeRepo("public"),
  wartremoverErrors in Compile ++= Warts.unsafe.filterNot(disableWarts.contains),

  // Release settings
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "com.spotify",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/dbeam")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/spotify/dbeam.git"),
    "scm:git:git@github.com:spotify/dbeam.git")),
  developers := List(
    Developer(id="labianchin", name="Luis Bianchin", email="labianchin@spotify.com", url=url("https://twitter.com/labianchin")),
    Developer(id="varjoranta", name="Hannu Varjoranta", email="varjo@spotify.com", url=url("https://twitter.com/hvarjoranta")),
    Developer(id="honnix", name="Hongxin Liang", email="honnix@spotify.com", url=url("https://spotify.com/"))
  )

)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

val disableWarts = Set(Wart.Null,
                       Wart.NonUnitStatements,
                       Wart.Throw,
                       Wart.DefaultArguments,
                       Wart.Var)


lazy val dbeam = project
  .in(file("dbeam"))
  .settings(commonSettings: _*)
  .settings(
    name := "dbeam-core",
    moduleName := "dbeam-core",
    description := "DBeam dumps from SQL databases using JDBC and Apache Beam",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.apache.commons" % "commons-dbcp2" % "2.1.1",
      "org.postgresql" % "postgresql" % "42.1.+",
      "mysql" % "mysql-connector-java" % "5.1.+",
      "com.google.cloud.sql" % "postgres-socket-factory" % "1.0.4",
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.0.4",
      "com.google.auto.value" % "auto-value" % autoValueVersion % "provided",
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "com.h2database" % "h2" % "1.4.196" % "test",
      "com.typesafe.slick" %% "slick" % "3.2.0" % "test"
    )
  )

// This project only depends on dbeam jar and adds pack wrapper scripts
val dbeamPack = project
  .in(file("dbeam-pack"))
  .settings(commonSettings: _*)
  .settings(noPublishSettings: _*)
  .enablePlugins(PackPlugin)
  .settings(packSettings: _*)
  .settings(
    name := "dbeam-pack",
    description := "DBeam dumps an SQL database using JDBC and Apache Beam",
    packageOptions in (Compile, packageBin) +=
      Package.ManifestAttributes( "Class-Path" ->
        (((managedClasspath in Runtime).value.files
          .map(f => f.getName)
          .filter(_.endsWith(".jar"))
          .mkString(" ")) + " " +
          (packageBin in Compile in dbeam).value.getName
          // add as run time dependency
          )
      ),
    packMain := Map(
      "jdbc-avro-job" -> "com.spotify.dbeam.JdbcAvroJob",
      "psql-avro-job" -> "com.spotify.dbeam.PsqlAvroJob"
    ),
    packJvmOpts := Map(
      "jdbc-avro-job" -> Seq("-Xmx512m"),
      "psql-avro-job" -> Seq("-Xmx512m")
    ),
    packJarNameConvention := "original",
    packGenerateWindowsBatFile := false
  )
  .dependsOn(dbeam)

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(noPublishSettings: _*)
  .settings(releaseSettings: _*)
  .settings(
    run := {
      (run in dbeamPack in Compile).evaluated
    }
  )
  .aggregate(dbeam, dbeamPack)

lazy val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease, // tag version in git,
    releaseStepCommand("publishSigned"),
    setNextVersion,
    commitNextVersion,
    releaseStepCommand("sonatypeReleaseAll"),
    pushChanges
  )
)


