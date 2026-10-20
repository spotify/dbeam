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

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

/**
 * Derives the Avro schema stored in the {@code parquet.avro.schema} footer key (and in {@code
 * _AVRO_SCHEMA.avsc}) from the Parquet {@link MessageType} that is actually used for writing, so
 * the two can never contradict each other.
 *
 * <p>The conversion is parquet-avro's {@link AvroSchemaConverter} with {@code
 * parquet.avro.add-list-element-records=false}, so 3-level lists become {@code array<T>} (or
 * {@code array<["null", T]>} for optional elements) without a wrapper record. Only the top-level
 * record is adjusted afterwards: its name, namespace and doc.
 */
public final class ParquetAvroSchema {

  private ParquetAvroSchema() {}

  /**
   * Converts {@code messageType} to an Avro record named after the message.
   *
   * @throws IllegalArgumentException for Parquet types without an Avro equivalent (e.g. INT96)
   * @throws org.apache.avro.SchemaParseException for names that are not valid Avro names
   */
  public static Schema toAvroSchema(
      final MessageType messageType, final String namespace, final String doc) {
    final Configuration conf = new Configuration(false);
    conf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    final Schema converted = new AvroSchemaConverter(conf).convert(messageType);
    final List<Schema.Field> fields = new ArrayList<>();
    for (final Schema.Field field : converted.getFields()) {
      fields.add(new Schema.Field(field, field.schema()));
    }
    final Schema record = Schema.createRecord(messageType.getName(), doc, namespace, false);
    record.setFields(fields);
    return record;
  }
}
