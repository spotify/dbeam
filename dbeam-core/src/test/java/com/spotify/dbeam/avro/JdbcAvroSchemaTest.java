package com.spotify.dbeam.avro;

import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcAvroSchemaTest {

  @Test
  public void checkTableNameForOrdinaryColumn() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("test_table");

    String expected = "test_table";
    String tableName = JdbcAvroSchema.getTableNameFromMetadata(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkTableNameForCastColumn() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getTableName(1)).thenReturn("");

    String expected = "no_table_name";
    String tableName = JdbcAvroSchema.getTableNameFromMetadata(meta);

    Assert.assertEquals(expected, tableName);
  }

  @Test
  public void checkTableNameForCastAndOrdinaryColumns() throws SQLException {
    ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getTableName(1)).thenReturn("");
    when(meta.getTableName(2)).thenReturn("test_table");

    String expected = "test_table";
    String tableName = JdbcAvroSchema.getTableNameFromMetadata(meta);

    Assert.assertEquals(expected, tableName);
  }
}
