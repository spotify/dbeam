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

import static org.mockito.Mockito.when;

import com.spotify.dbeam.options.DBeamPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BeamJdbcAvroSchemaTest {

  @Test
  public void verifyTableNameWrongType() {
    String expected = "unknown";
    PipelineOptions args = Mockito.mock(PipelineOptions.class);

    String actual = BeamJdbcAvroSchema.getTablename(args);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void verifyTableNameReturnsNull() {
    String expected = "unknown";
    DBeamPipelineOptions args = Mockito.mock(DBeamPipelineOptions.class);
    when(args.getTable()).thenReturn(null);

    String actual = BeamJdbcAvroSchema.getTablename(args);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void verifyTableNameReturnsEmpty() {
    String expected = "unknown";
    DBeamPipelineOptions args = Mockito.mock(DBeamPipelineOptions.class);
    when(args.getTable()).thenReturn("");

    String actual = BeamJdbcAvroSchema.getTablename(args);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void verifyTableNameOk() {
    String expected = "abba";
    DBeamPipelineOptions args = Mockito.mock(DBeamPipelineOptions.class);
    when(args.getTable()).thenReturn(expected);

    String actual = BeamJdbcAvroSchema.getTablename(args);

    Assert.assertEquals(expected, actual);
  }
}
