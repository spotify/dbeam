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

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroSchemaMetadataProviderTest {

  @BeforeClass
  public static void beforeAll() {}

  public static Schema createProvidedSchemaWithDoc(
      final List<String> fieldNames, final String schemaDoc) {
    return createProvidedSchema(fieldNames, "providedSchemaName", schemaDoc);
  }

  public static Schema createProvidedSchemaWithName(
      final List<String> fieldNames, final String schemaName) {
    return createProvidedSchema(fieldNames, schemaName, "providedSchemaDoc");
  }

  public static Schema createProvidedSchema(final List<String> fieldNames) {
    return createProvidedSchema(fieldNames, "providedSchemaName", "providedSchemaDoc");
  }

  public static Schema createProvidedSchema(
      final List<String> fieldNames, final String schemaName, final String schemaDoc) {
    final SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record(schemaName)
            .namespace("providedSchemaNamespace")
            .doc(schemaDoc)
            .fields();

    fieldNames.forEach(
        fieldName ->
            builder.name(fieldName).doc("Doc for " + fieldName).type().stringType().noDefault());
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

    List<String> fieldNames = Arrays.asList("field1", "field2");
    Schema providedSchema = createProvidedSchema(fieldNames);

    AvroSchemaMetadataProvider provider =
        new AvroSchemaMetadataProvider(providedSchema, null, "schemaNamespace", null);

    Assert.assertEquals("providedSchemaName", provider.avroSchemaName("temp1"));
    Assert.assertEquals("providedSchemaNamespace", provider.avroSchemaNamespace());
    Assert.assertEquals("providedSchemaDoc", provider.avroDoc("temp2"));
    Assert.assertEquals("Doc for field1", provider.getFieldDoc("field1", "dummy"));
    Assert.assertEquals("Doc for field2", provider.getFieldDoc("field2", "dummy"));
    Assert.assertEquals("default", provider.getFieldDoc("field3", "default"));
  }

  private static AvroSchemaMetadataProvider getAvroSchemaMetadataProviderWithDoc(
      String providedSchemaDoc, String commandLineSchemaDoc) {
    List<String> fieldNames = Arrays.asList("field1", "field2");
    Schema providedSchema = createProvidedSchemaWithDoc(fieldNames, providedSchemaDoc);

    return new AvroSchemaMetadataProvider(
        providedSchema, null, "schemaNamespace", commandLineSchemaDoc);
  }

  @Test
  public void verifyProviderWithSchemaAndNoSchemaDocUsesCommandLineDoc() {

    final String providedSchemaDoc = null; // not provided
    final String commandLineSchemaDoc = "Custom Doc";
    final String generatedSchemaDoc = "GeneratedDoc";
    final AvroSchemaMetadataProvider provider =
        getAvroSchemaMetadataProviderWithDoc(providedSchemaDoc, commandLineSchemaDoc);

    Assert.assertEquals(commandLineSchemaDoc, provider.avroDoc(generatedSchemaDoc));
  }

  @Test
  public void verifyProviderWithSchemaAndEmptySchemaDocUsesSchemaDoc() {

    final String providedSchemaDoc = ""; // empty
    final String commandLineSchemaDoc = "Custom Doc";
    final String generatedSchemaDoc = "GeneratedDoc";
    final AvroSchemaMetadataProvider provider =
        getAvroSchemaMetadataProviderWithDoc(providedSchemaDoc, commandLineSchemaDoc);

    Assert.assertEquals(providedSchemaDoc, provider.avroDoc(generatedSchemaDoc));
  }

  @Test
  public void verifyProviderWithSchemaAndNoSchemaDocAndNoCommandLineDocUsesGeneratedDoc() {

    final String providedSchemaDoc = null; // not provided
    final String commandLineSchemaDoc = null;
    final String generatedSchemaDoc = "GeneratedDoc";
    final AvroSchemaMetadataProvider provider =
        getAvroSchemaMetadataProviderWithDoc(providedSchemaDoc, commandLineSchemaDoc);

    Assert.assertEquals(generatedSchemaDoc, provider.avroDoc(generatedSchemaDoc));
  }

  private static AvroSchemaMetadataProvider getAvroSchemaMetadataProviderWithName(
      String providedSchemaName, String commandLineSchemaName) {
    List<String> fieldNames = Arrays.asList("field1", "field2");
    Schema providedSchema = createProvidedSchemaWithName(fieldNames, providedSchemaName);

    return new AvroSchemaMetadataProvider(
        providedSchema, commandLineSchemaName, "schemaNamespace", "DummyDoc");
  }

  @Test(expected = NullPointerException.class)
  public void verifyProviderWithSchemaAndNoSchemaNameThrowsException() {

    final String providedSchemaName = null; // not provided
    final String commandLineSchemaName = "Custom Name";
    final String generatedSchemaName = "GeneratedName";
    getAvroSchemaMetadataProviderWithName(providedSchemaName, commandLineSchemaName);
  }

  @Test
  public void verifyProviderWithSchemaAndNoSchemaNameAndNoCommandLineNameUsesGeneratedName() {

    final String providedSchemaName = "ProvidedName";
    final String commandLineSchemaName = "Custom Name";
    final String generatedSchemaName = "GeneratedName";
    final AvroSchemaMetadataProvider provider =
        getAvroSchemaMetadataProviderWithName(providedSchemaName, commandLineSchemaName);

    Assert.assertEquals(providedSchemaName, provider.avroSchemaName(generatedSchemaName));
  }
}
