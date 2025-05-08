package com.spotify.dbeam.options;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class ArrayHandlingModeTest {
  @Test
  public void validValuesShouldPass() {
    Assert.assertEquals(ArrayHandlingMode.Bytes,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.Bytes));
    Assert.assertEquals(ArrayHandlingMode.TypedMetaFromFirstRow,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.TypedMetaFromFirstRow));
    Assert.assertEquals(ArrayHandlingMode.TypedMetaPostgres,
        ArrayHandlingMode.validateValue(ArrayHandlingMode.TypedMetaPostgres));
  }

  @Test
  public void invalidValueShouldThrow() {
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> ArrayHandlingMode.validateValue("invalid"));
    Assert.assertEquals("Invalid value 'invalid' for array handling mode. Allowed values: "
        + "[bytes, typed_first_row, typed_postgres]",
        thrown.getMessage());
  }
}
