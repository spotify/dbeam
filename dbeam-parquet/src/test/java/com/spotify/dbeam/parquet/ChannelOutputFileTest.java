/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2025 Spotify AB
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

package com.spotify.dbeam.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.Assert;
import org.junit.Test;

public class ChannelOutputFileTest {

  @Test
  public void shouldTrackPosition() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    final ChannelOutputFile outputFile = new ChannelOutputFile(channel);

    try (PositionOutputStream out = outputFile.create(0)) {
      Assert.assertEquals(0, out.getPos());

      out.write(42);
      Assert.assertEquals(1, out.getPos());

      out.write(new byte[] {1, 2, 3, 4, 5});
      Assert.assertEquals(6, out.getPos());

      out.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, 2, 3);
      Assert.assertEquals(9, out.getPos());
    }

    Assert.assertEquals(9, baos.size());
  }

  @Test
  public void shouldWriteCorrectBytes() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    final ChannelOutputFile outputFile = new ChannelOutputFile(channel);

    try (PositionOutputStream out = outputFile.create(0)) {
      out.write(65); // 'A'
      out.write(new byte[] {66, 67}); // 'B', 'C'
    }

    byte[] result = baos.toByteArray();
    Assert.assertEquals(3, result.length);
    Assert.assertEquals(65, result[0]);
    Assert.assertEquals(66, result[1]);
    Assert.assertEquals(67, result[2]);
  }

  @Test
  public void shouldNotSupportBlockSize() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    final ChannelOutputFile outputFile = new ChannelOutputFile(channel);

    Assert.assertFalse(outputFile.supportsBlockSize());
    Assert.assertEquals(0, outputFile.defaultBlockSize());
  }

  @Test
  public void shouldReturnSameStreamFromCreateOrOverwrite() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    final ChannelOutputFile outputFile = new ChannelOutputFile(channel);

    try (PositionOutputStream out = outputFile.createOrOverwrite(0)) {
      Assert.assertNotNull(out);
      Assert.assertEquals(0, out.getPos());
    }
  }
}
