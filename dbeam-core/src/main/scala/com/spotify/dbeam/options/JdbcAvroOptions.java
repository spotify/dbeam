/*
 * Copyright 2018 Spotify AB.
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
package com.spotify.dbeam.options;

import com.google.auto.value.AutoValue;

import org.apache.avro.file.CodecFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;

import javax.annotation.Nullable;

@AutoValue
public abstract class JdbcAvroOptions implements Serializable {
  public abstract JdbcConnectionConfiguration getJdbcConnectionConfiguration();
  @Nullable public abstract StatementPreparator getStatementPreparator();
  public abstract int getFetchSize();
  public abstract String getAvroCodec();

  abstract Builder builder();

  public CodecFactory getCodecFactory() {
    if (getAvroCodec().equals("snappy")) {
      return CodecFactory.snappyCodec();
    } else if (getAvroCodec().startsWith("deflate")) {
      return CodecFactory.deflateCodec(Integer.valueOf(getAvroCodec().replace("deflate", "")));
    }
    throw new IllegalArgumentException("Invalid avroCodec " + getAvroCodec());
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setJdbcConnectionConfiguration(JdbcConnectionConfiguration jdbcConnectionConfiguration);
    abstract Builder setStatementPreparator(StatementPreparator statementPreparator);
    abstract Builder setFetchSize(int fetchSize);
    abstract Builder setAvroCodec(String avroCodec);
    abstract JdbcAvroOptions build();
  }

  public static JdbcAvroOptions create(JdbcConnectionConfiguration jdbcConnectionConfiguration,
                                       int fetchSize, String avroCodec) {
    return new AutoValue_JdbcAvroOptions.Builder()
        .setJdbcConnectionConfiguration(jdbcConnectionConfiguration)
        .setFetchSize(fetchSize)
        .setAvroCodec(avroCodec)
        .build();
  }

  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }
}
