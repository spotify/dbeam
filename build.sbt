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

import com.typesafe.sbt.SbtGit.GitKeys._
import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version

val scioVersion = "0.4.3"
val beamVersion = "2.1.0"
val slf4jVersion = "1.7.13"
val autoValueVersion = "1.3"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.spotify.data",
  scalaVersion := "2.11.11",
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),

  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",

  // Repositories and dependencies
  resolvers ++= Seq(
    "Concurrent Maven Repo" at "http://conjars.org/repo",
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
  ),
  wartremoverErrors in Compile ++= Warts.unsafe.filterNot(disableWarts.contains),
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

val disableWarts = Set(Wart.Null,
                       Wart.NonUnitStatements,
                       Wart.Throw,
                       Wart.DefaultArguments,
                       Wart.Var)


// TODO: move this project to open source
lazy val dbeam = project
  .in(file("dbeam"))
  .settings(commonSettings: _*)
  .settings(
    name := "dbeam",
    description := "DBeam dumps from SQL databases using JDBC and Apache Beam",
    unmanagedResourceDirectories in Compile += baseDirectory.value / "../bin",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.commons" % "commons-dbcp2" % "2.1.1",
      "org.postgresql" % "postgresql" % "42.1.+",
      "mysql" % "mysql-connector-java" % "5.1.+",
      "com.google.cloud.sql" % "postgres-socket-factory" % "1.0.3",
      "com.google.cloud.sql" % "mysql-socket-factory" % "1.0.3",
      "com.google.auto.value" % "auto-value" % autoValueVersion % "provided",
      "com.spotify" %% "scio-test" % scioVersion % "test",
      "com.h2database" % "h2" % "1.4.196" % "test",
      "com.typesafe.slick" %% "slick" % "3.2.0" % "test"
    )
  )

// This project only depends on dbeam jar and adds pack wrapper scripts
val dbeamPack = project
  .in(file("dbeam_pack"))
  .settings(commonSettings: _*)
  .settings(packAutoSettings: _*)
  .settings(
    name := "dbeam_pack",
    description := "DBeam dumps an SQL database using JDBC and Apache Beam",
    //unmanagedResourceDirectories in Compile += baseDirectory.value / "../bin",
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
  .settings(releaseSettings: _*)
  .settings(
    run := {
      (run in dbeamPack in Compile).evaluated
    }
  )
  .aggregate(dbeam, dbeamPack)

lazy val releaseSettings = Seq(
  releaseIgnoreUntrackedFiles := true,  // TODO: remove this
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease, // tag version in git,
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)


