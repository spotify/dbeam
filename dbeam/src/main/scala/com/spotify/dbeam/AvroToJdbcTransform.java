package com.spotify.dbeam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;

/**
 * Beam transform (Java) that receives a PCollection of Avro GenericRecords, which will be
 * written into a database in batches within a transaction utilizing JDBC.
 */
public class AvroToJdbcTransform extends PTransform<PCollection<GenericRecord>, PCollection<PDone>> {

  @Override
  public PCollection<PDone> expand(PCollection<GenericRecord> input) {
    // Todo implementation
    System.out.println("Applying transformation in AvroToJdbcTransform.expand");
    return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
        input.getWindowingStrategy(), IsBounded.BOUNDED);
  }

}
