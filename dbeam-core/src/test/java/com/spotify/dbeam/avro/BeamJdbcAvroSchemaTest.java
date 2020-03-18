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
