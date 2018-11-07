package com.spotify.dbeam;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule$;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

public class BeamHelper {

  public static PipelineResult waitUntilDone(PipelineResult result) {
    PipelineResult.State state  = result.waitUntilFinish();
    if (!state.equals(PipelineResult.State.DONE)) {
      throw new Pipeline.PipelineExecutionException(
          new Exception("Job finished with state " + state.toString()));
    }
    return result;
  }

  public static void writeToFile(String filename, ByteBuffer contents) throws IOException {
    ResourceId resourceId = FileSystems.matchNewResource(filename, false);
    try (WritableByteChannel out = FileSystems.create(resourceId, MimeTypes.TEXT)) {
      out.write(contents);
    }
  }

  public static void saveStringOnSubPath(String path, String subPath, String contents) throws IOException {
    String filename = path.replaceAll("/+$", "") + subPath;
    writeToFile(filename, ByteBuffer.wrap(contents.getBytes(Charset.defaultCharset())));
  }

}
