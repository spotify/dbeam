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

package com.spotify.dbeam.args;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.PreparedStatement;

import javax.annotation.Nullable;

import org.apache.avro.file.CodecFactory;

@AutoValue
public abstract class JdbcAvroArgs implements Serializable {

  public abstract JdbcConnectionArgs jdbcConnectionConfiguration();

  @Nullable public abstract StatementPreparator statementPreparator();

  public abstract int fetchSize();

  public abstract String avroCodec();

  abstract Builder builder();

  public CodecFactory getCodecFactory() {
    if (avroCodec().equals("snappy")) {
      return CodecFactory.snappyCodec();
    } else if (avroCodec().startsWith("deflate")) {
      return CodecFactory.deflateCodec(Integer.valueOf(avroCodec().replace("deflate", "")));
    }
    throw new IllegalArgumentException("Invalid avroCodec " + avroCodec());
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setJdbcConnectionConfiguration(JdbcConnectionArgs jdbcConnectionArgs);

    abstract Builder setStatementPreparator(StatementPreparator statementPreparator);

    abstract Builder setFetchSize(int fetchSize);

    abstract Builder setAvroCodec(String avroCodec);

    abstract JdbcAvroArgs build();
  }

  public static JdbcAvroArgs create(JdbcConnectionArgs jdbcConnectionArgs,
                                    int fetchSize, String avroCodec) {
    Preconditions.checkArgument(avroCodec.matches("snappy|deflate[1-9]"),
                          "Avro codec should be snappy or deflate1, .., deflate9");
    return new AutoValue_JdbcAvroArgs.Builder()
        .setJdbcConnectionConfiguration(jdbcConnectionArgs)
        .setFetchSize(fetchSize)
        .setAvroCodec(avroCodec)
        .build();
  }

  public static JdbcAvroArgs create(JdbcConnectionArgs jdbcConnectionArgs) {
    return create(jdbcConnectionArgs, 10000, "deflate6");
  }

  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }
}
