package com.spotify.dbeam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class BeamHelper {

  public static PipelineResult waitUntilDone(PipelineResult result) {
    PipelineResult.State state  = result.waitUntilFinish();
    if (!state.equals(PipelineResult.State.DONE)) {
      throw new Pipeline.PipelineExecutionException(
          new Exception("Job finished with state " + state.toString()));
    }
    return result;
  }
}
