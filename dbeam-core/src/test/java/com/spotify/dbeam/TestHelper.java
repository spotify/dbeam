/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestHelper {

  public static Path createTmpDirPath(final String subNamePrefix) throws IOException {
    final Path path = Paths.get(System.getProperty("java.io.tmpdir"),
        subNamePrefix + "-" + UUID.randomUUID().toString());
    Files.createDirectories(path);
    path.toFile().deleteOnExit();
    return path;
  }

  public static List<String> listDir(File dir) {
    return Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .map(File::getName).sorted().collect(Collectors.toList());
  }

  public static UUID byteBufferToUuid(final ByteBuffer byteBuffer) {
    Long high = byteBuffer.getLong();
    Long low = byteBuffer.getLong();

    return new UUID(high, low);
  }

}
