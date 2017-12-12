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

package com.spotify.dbeam.options

import java.net.{InetAddress, URI}
import java.sql.{Connection, DriverManager}

import org.joda.time.{DateTime, ReadablePeriod}

import scala.util.matching.Regex

trait JdbcConnectionArgs {
  def driverClass: String

  def connectionUrl: String

  def username: String

  def password: String

  def createConnection(): Connection = {
    Class.forName(driverClass)
    DriverManager.getConnection(connectionUrl, username, password)
  }

  /**
    * Resolve host address outside the Beam/Dataflow worker
    * This is favorable since it is currently no easy to configure DNS server on workers
    */
  def resolvedConnectionUrl: String = {
    val uri: URI = URI.create(connectionUrl)
    val host: String = uri.getHost
    val hostAddress: String = InetAddress.getByName(host).getHostAddress
    connectionUrl.replaceFirst("://" + host, "://" + hostAddress)
  }
}

object JdbcConnectionUtil {
  private val driverMapping = Map(
    "postgresql" -> "org.postgresql.Driver",
    "mysql" -> "com.mysql.jdbc.Driver",
    "h2" -> "org.h2.Driver"
  )

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def getDriverClass(url: String): String = {
    val parts: Array[String] = url.split(":", 3)
    require(parts(0) == "jdbc",
      s"Invalid jdbc connection URL: $url. Expect jdbc:postgresql or jdbc:mysql as prefix.")
    val mappedClass: Option[String] = driverMapping.get(parts(1))
      .map(Class.forName(_).getCanonicalName)
    require(mappedClass.isDefined,
      s"Invalid jdbc connection URL: $url. Expect jdbc:postgresql or jdbc:mysql as prefix.")
    mappedClass.get
  }
}

trait TableArgs {
  private val validTableName: Regex = "^[a-zA-Z_][a-zA-Z0-9_]*$".r

  def tableName: String

  def checkTableName(): Boolean = {
    validTableName.pattern.matcher(tableName).matches
  }
}

trait QueryArgs extends TableArgs {
  def limit: Option[Int]

  def partitionColumn: Option[String]

  def partition: Option[DateTime]

  def partitionPeriod: ReadablePeriod

  def buildQueries(): Iterable[String] = {
    val limit = this.limit.map(" LIMIT %d".format(_)).getOrElse("")
    val where = (partition, partitionColumn) match {
      case (Some(partition), Some(partitionColumn)) =>
        val datePartition = partition.toLocalDate
        val nextPartition = datePartition.plus(partitionPeriod).toString
        s" WHERE $partitionColumn >= '$datePartition'" +
          s" AND $partitionColumn < '$nextPartition'"
      case _ => ""
    }
    Seq(s"SELECT * FROM $tableName$where$limit")
  }
}
