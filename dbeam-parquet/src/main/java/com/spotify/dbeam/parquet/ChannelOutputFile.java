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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * An {@link OutputFile} implementation backed by a {@link WritableByteChannel},
 * enabling Parquet writing without Hadoop dependencies.
 */
public class ChannelOutputFile implements OutputFile {

  private final WritableByteChannel channel;

  public ChannelOutputFile(WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new ChannelPositionOutputStream(Channels.newOutputStream(channel));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  @Override
  public String getPath() {
    return channel.toString();
  }

  static class ChannelPositionOutputStream extends PositionOutputStream {

    private final OutputStream out;
    private long position;

    ChannelPositionOutputStream(OutputStream out) {
      this.out = out;
      this.position = 0;
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      position++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
      position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }
}
