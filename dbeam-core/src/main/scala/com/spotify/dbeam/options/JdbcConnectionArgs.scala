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
}

object JdbcConnectionUtil {
  private val driverMapping = Map(
    "postgresql" -> "org.postgresql.Driver",
    "mysql" -> "com.mysql.jdbc.Driver",
    "h2" -> "org.h2.Driver",
    "sqlserver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  )

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def getDriverClass(url: String): String = {
    val parts: Array[String] = url.split(":", 3)
    val mappedClass: Option[String] = if (parts(0) == "jdbc") {
      driverMapping.get(parts(1))
      .map(Class.forName(_).getCanonicalName)
    } else {
      None
    }
    mappedClass
      .getOrElse(
        throw new IllegalArgumentException(
          s"Invalid jdbc connection URL: $url. Expect jdbc:postgresql," +
            s" jdbc:mysql or jdbc:sqlserver as prefix."))
  }

  def isSqlServer(url: String): Boolean = {
    Some(JdbcConnectionUtil.getDriverClass(url)) == driverMapping.get("sqlserver")
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

  def buildQueries(url: String): Iterable[String] = {
    val isSqlServer: Boolean = JdbcConnectionUtil.isSqlServer(url)
    val limit = if (!isSqlServer) this.limit.map(" LIMIT %d".format(_)).getOrElse("") else ""
    val top = if (isSqlServer) this.limit.map(" TOP %d".format(_)).getOrElse("") else ""
    val where = (partition, partitionColumn) match {
      case (Some(partition), Some(partitionColumn)) =>
        val datePartition = partition.toLocalDate
        val nextPartition = datePartition.plus(partitionPeriod).toString
        s" WHERE $partitionColumn >= '$datePartition'" +
          s" AND $partitionColumn < '$nextPartition'"
      case _ => ""
    }
    Seq(s"SELECT$top * FROM $tableName$where$limit")
  }
}
