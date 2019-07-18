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

package com.spotify.dbeam

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

object TestHelper {
        
  def createTmpDirName(subDirNamePrefix: String): Path = {
    val dir = Paths.get(sys.props("java.io.tmpdir"),  subDirNamePrefix + "-" + UUID.randomUUID().toString)
    dir         
  }

  def createDir(path: Path): Path = {
    Files.createDirectories(path)
  }

  def createFile(path: Path): Path = {
    Files.createFile(path)
  }

  def writeToFile(path: Path, content: String): Unit = {
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }
}
