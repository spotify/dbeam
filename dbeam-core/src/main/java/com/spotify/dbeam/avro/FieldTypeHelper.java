/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2023 Spotify AB
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
import org.apache.avro.SchemaBuilder;

public class FieldTypeHelper {
  public static SchemaBuilder.FieldAssembler<Schema> setStringType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.stringType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().stringType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setIntType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.intType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().intType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setLongType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.longType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().longType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setLongLogicalType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.longBuilder().prop("logicalType", "timestamp-millis").endLong().noDefault()
        : field
            .unionOf()
            .nullBuilder()
            .endNull()
            .and()
            .longBuilder()
            .prop("logicalType", "timestamp-millis")
            .endLong()
            .endUnion()
            .nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setBytesType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.bytesType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().bytesType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setBooleanType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.booleanType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().booleanType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setFloatType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.floatType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().floatType().endUnion().nullDefault();
  }

  public static SchemaBuilder.FieldAssembler<Schema> setDoubleType(
      final SchemaBuilder.FieldTypeBuilder<Schema> field, final boolean useNotNullTypes) {
    return useNotNullTypes
        ? field.floatType().noDefault()
        : field.unionOf().nullBuilder().endNull().and().floatType().endUnion().nullDefault();
  }
}
