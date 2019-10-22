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

import java.util.Optional;
import org.apache.avro.Schema;

public class InputAvroSchenaWrapper {

  private final Optional<Schema> inputSchema;
  
  private InputAvroSchenaWrapper(final Optional<Schema> inputSchema) {
    this.inputSchema = inputSchema;  
  }
  
  public static InputAvroSchenaWrapper of(final Optional<Schema> inputSchema) {
    return new InputAvroSchenaWrapper(inputSchema);  
  }
  
  public String getRecordDoc(final String fallback) {
    return inputSchema.map(Schema::getDoc).orElse(fallback);
  }

  public String getRecordNamespace(final String fallback) {
    return inputSchema.map(Schema::getNamespace).orElse(fallback);
  }

  public String getFieldDoc(final String fieldName, final String fallback) {
    return inputSchema
        .flatMap(e -> Optional.ofNullable(e.getField(fieldName)).map(Schema.Field::doc))
        .orElse(fallback);
  }
}
