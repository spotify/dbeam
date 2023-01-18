/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.dbeam.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchemaMetadataProvider {

  private static Logger LOGGER = LoggerFactory.getLogger(AvroSchemaMetadataProvider.class);

  // provided schema has a priority over single arguments' values.
  private final Schema provided;
  private final String avroSchemaName;
  private final String avroSchemaNamespace;
  private final String avroDoc;

  public AvroSchemaMetadataProvider(
      final String avroSchemaName, final String avroSchemaNamespace, final String avroDoc) {
    this(null, avroSchemaName, avroSchemaNamespace, avroDoc);
  }

  public AvroSchemaMetadataProvider(
      final Schema provided,
      final String avroSchemaName,
      final String avroSchemaNamespace,
      final String avroDoc) {
    this.provided = provided;
    this.avroSchemaName = avroSchemaName;
    this.avroSchemaNamespace = avroSchemaNamespace;
    this.avroDoc = avroDoc;
  }

  public String avroDoc(final String defaultVal) {
    return (provided != null) ? provided.getDoc() : (avroDoc != null) ? avroDoc : defaultVal;
  }

  public String avroSchemaName(final String defaultVal) {
    if (provided != null) {
      String name = provided.getName();
      return (name != null) ? name : defaultVal;
    } else {
      return avroSchemaName != null ? avroSchemaName : defaultVal;
    }
  }

  public String avroSchemaNamespace() {
    return (provided != null) ? provided.getNamespace() : avroSchemaNamespace;
  }

  public String getFieldDoc(final String fieldName, final String defaultVal) {
    if (provided != null) {
      final Schema.Field field = provided.getField(fieldName);
      if (field != null) {
        return field.doc();
      } else {
        LOGGER.warn("Field [{}] not found in the provided schema", fieldName);
        return defaultVal;
      }
    } else {
      return defaultVal;
    }
  }
}
