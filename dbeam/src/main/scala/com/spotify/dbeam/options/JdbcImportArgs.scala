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

import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.apache.beam.sdk.options.PipelineOptionsFactory

case class JdbcImportArgs(driverClass: String,
                          connectionUrl: String,
                          username: String,
                          password: String,
                          tableName: String,
                          input: String,
                          format: String)
  extends JdbcConnectionArgs with TableArgs {
  require(checkTableName(), s"Invalid SQL table name: $tableName")
}

object JdbcImportArgs {

  def contextAndArgs(cmdlineArgs: Array[String]): (ScioContext, JdbcImportArgs) = {
    PipelineOptionsFactory.register(classOf[JdbcImportPipelineOptions])
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdlineArgs)
    val importOptions = sc.options.as(classOf[JdbcImportPipelineOptions])

    require(importOptions.getConnectionUrl != null, "'connectionUrl' must be defined")
    require(importOptions.getInput != null, "'input' must be defined")
    require(importOptions.getTable != null, "'table' must be defined")

    val importArgs: JdbcImportArgs = JdbcImportArgs(
      JdbcConnectionUtil.getDriverClass(importOptions.getConnectionUrl),
      importOptions.getConnectionUrl,
      importOptions.getUsername,
      PipelineOptionsUtil.readPassword(importOptions).orNull,
      importOptions.getTable,
      importOptions.getInput,
      importOptions.getFormat)

    (sc, importArgs)
  }

}
