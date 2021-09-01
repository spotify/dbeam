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

package com.spotify.dbeam.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroSchemaMetadataProviderTest {

  @BeforeClass
  public static void beforeAll() {}

  public static Schema createProvidedSchema() {
    final SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record("providedSchemaName")
            .namespace("providedSchemaNamespace")
            .doc("providedSchemaDoc")
            .fields();

    for (int i = 1; i <= 2; i++) {
      String fieldName = "field" + i;
      builder.name(fieldName).doc("Doc for " + fieldName).type().stringType().noDefault();
    }
    return builder.endRecord();
  }

  @Test
  public void verifySimpleProvider() {

    AvroSchemaMetadataProvider provider =
        new AvroSchemaMetadataProvider("schemaName", "schemaNamespace", "avroDoc");

    Assert.assertEquals("schemaName", provider.avroSchemaName(""));
    Assert.assertEquals("schemaNamespace", provider.avroSchemaNamespace());
    Assert.assertEquals("avroDoc", provider.avroDoc(""));
  }

  @Test
  public void verifyEmptyProvider() {

    AvroSchemaMetadataProvider provider =
        new AvroSchemaMetadataProvider(null, "schemaNamespace", null);

    Assert.assertEquals("temp1", provider.avroSchemaName("temp1"));
    Assert.assertEquals("schemaNamespace", provider.avroSchemaNamespace());
    Assert.assertEquals("temp2", provider.avroDoc("temp2"));
  }

  @Test
  public void verifyProviderWithSchema() {

    Schema providedSchema = createProvidedSchema();

    AvroSchemaMetadataProvider provider =
        new AvroSchemaMetadataProvider(providedSchema, null, "schemaNamespace", null);

    Assert.assertEquals("providedSchemaName", provider.avroSchemaName("temp1"));
    Assert.assertEquals("providedSchemaNamespace", provider.avroSchemaNamespace());
    Assert.assertEquals("providedSchemaDoc", provider.avroDoc("temp2"));
    Assert.assertEquals("Doc for field1", provider.getFieldDoc("field1", "dummy"));
    Assert.assertEquals("Doc for field2", provider.getFieldDoc("field2", "dummy"));
    Assert.assertEquals("default", provider.getFieldDoc("field3", "default"));
  }
}
