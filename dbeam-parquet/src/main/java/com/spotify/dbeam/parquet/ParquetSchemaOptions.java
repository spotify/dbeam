/*-
 * -\-\-
 * DBeam Parquet
 * --
 * Copyright (C) 2016 - 2026 Spotify AB
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

import com.google.auto.value.AutoValue;
import com.spotify.dbeam.options.ArrayHandlingMode;
import java.io.Serializable;

/**
 * How JDBC columns map to Parquet: shared by {@link JdbcParquetSchema} (the schema) and {@link
 * JdbcParquetWriteSupport} (the values), so the two cannot disagree on a flag.
 */
@AutoValue
public abstract class ParquetSchemaOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  /** {@code --useLogicalTypes}: annotate timestamps and uuids with logical types. */
  public abstract boolean useLogicalTypes();

  /** {@code --useTimestampMicros}: timestamp/date/time columns as epoch micros, not millis. */
  public abstract boolean useTimestampMicros();

  /** {@code --arrayMode}, see {@link ArrayHandlingMode}. */
  public abstract String arrayMode();

  /** {@code --parquetNativeTypes}: native Parquet DATE, TIME and DECIMAL types. */
  public abstract boolean nativeTypes();

  /** {@code --nullableArrayItems}: typed LIST elements are {@code optional}, else required. */
  public abstract boolean nullableArrayItems();

  /**
   * {@code --parquetUnsupportedArrayItemsAsString}: LIST elements without a Parquet mapping (e.g.
   * numeric, timestamp, date, json) become {@code binary (STRING)} instead of failing.
   */
  public abstract boolean unsupportedArrayItemsAsString();

  /**
   * Allow columns whose normalized names collide. Only for the schema derived to validate a
   * {@code --parquetSchemaFilePath} schema, which matches fields by position and supplies its own
   * names; an exported schema always fails on collisions.
   */
  public abstract boolean allowDuplicateFieldNames();

  public abstract Builder toBuilder();

  /** All flags off, {@code --arrayMode=typed_first_row}. */
  public static Builder builder() {
    return new AutoValue_ParquetSchemaOptions.Builder()
        .setUseLogicalTypes(false)
        .setUseTimestampMicros(false)
        .setArrayMode(ArrayHandlingMode.TypedMetaFromFirstRow)
        .setNativeTypes(false)
        .setNullableArrayItems(false)
        .setUnsupportedArrayItemsAsString(false)
        .setAllowDuplicateFieldNames(false);
  }

  /** Defaults with the given {@code --arrayMode}. */
  public static ParquetSchemaOptions of(final String arrayMode) {
    return builder().setArrayMode(arrayMode).build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUseLogicalTypes(boolean value);

    public abstract Builder setUseTimestampMicros(boolean value);

    public abstract Builder setArrayMode(String value);

    public abstract Builder setNativeTypes(boolean value);

    public abstract Builder setNullableArrayItems(boolean value);

    public abstract Builder setUnsupportedArrayItemsAsString(boolean value);

    public abstract Builder setAllowDuplicateFieldNames(boolean value);

    public abstract ParquetSchemaOptions build();
  }
}
