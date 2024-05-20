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

package com.spotify.dbeam.jobs;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ObjectArrays;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Base64;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.management.ObjectName;
import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchFooJob {
  private static final Logger LOG = LoggerFactory.getLogger(BenchFooJob.class);

  private static final Counter totalProcessElementMs =
      Metrics.counter(BenchFooJob.class.getCanonicalName(), "totalProcessElementMs");
  private static final Distribution processElementMs =
      Metrics.distribution(BenchFooJob.class.getCanonicalName(), "processElementMs");
  private PipelineResult pipelineResult;

  public interface BenchJdbcAvroOptions extends PipelineOptions {
    @Description("Number of elements in the input.")
    @Default.Integer(1000)
    int getExecutions();

    void setExecutions(int value);
  }

  private final PipelineOptions pipelineOptions;
  private final Pipeline pipeline;
  private final DataflowClient dataflowClient;

  public BenchFooJob(final PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.pipeline = Pipeline.create(pipelineOptions);
    this.dataflowClient = DataflowClient.create(pipelineOptions.as(DataflowPipelineOptions.class));
  }

  public static BenchFooJob create(final String[] cmdLineArgs) {
    PipelineOptionsFactory.register(BenchJdbcAvroOptions.class);
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(cmdLineArgs).withValidation().create();
    return new BenchFooJob(options);
  }

  public PipelineResult run() {
    final int executions = pipelineOptions.as(BenchJdbcAvroOptions.class).getExecutions();
    pipeline
        .apply(
            "InputRange",
            Create.of(IntStream.range(1, executions + 1).boxed().collect(Collectors.toList())))
        .apply("GroupByKeyReshuffle", Reshuffle.viaRandomKey())
        .apply("DoHeavyProcessing", ParDo.of(new PT1(5000)));

    this.pipelineResult = this.pipeline.run();
    return pipelineResult;
  }

  public PipelineResult waitUntilFinish() {
    Preconditions.checkNotNull(this.pipelineResult);
    pipelineResult.waitUntilFinish();
    LOG.info(
        "Metrics for {}: {}",
        this.pipelineOptions.getJobName(),
        pipelineResult.metrics().allMetrics().toString());
    try {
      LOG.info(
          "Dataflow Metrics for {}: {}",
          this.pipelineOptions.getJobName(),
          this.dataflowClient
              .getJobMetrics(((DataflowPipelineJob) pipelineResult).getJobId())
              .getMetrics()
              .toString());
    } catch (IOException e) {
      LOG.error("??", e);
    }
    return pipelineResult;
  }

  public static void main(final String[] cmdLineArgs) {
    final String[] baseArgs =
        ObjectArrays.concat(
            cmdLineArgs,
            new String[] {
              "--runner=DataflowRunner",
              "--project=spotify-dbeam",
              "--region=europe-west4",
              "--tempLocation=gs://spotify-dbeam-demo-output-n1tk7n/bench1",
              "--autoscalingAlgorithm=NONE",
              "--numWorkers=5",
              "--executions=100000"
            },
            String.class);
    final Stream<String[]> jobParams =
        Stream.of(
            new String[] {
              "--jobName=v4-n1-java21",
              "--workerMachineType=n1-standard-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-n2d-java21",
              "--workerMachineType=n2d-standard-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-n2-java21",
              "--workerMachineType=n2-standard-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-e2-java21",
              "--workerMachineType=e2-standard-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-n1-highmem-java21",
              "--workerMachineType=n1-highmem-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-n2-highmem-java21",
              "--workerMachineType=n2-highmem-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-n2d-highmem-java21",
              "--workerMachineType=n2d-highmem-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-e2-highmem-java21",
              "--workerMachineType=e2-highmem-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-t2a-java21",
              "--workerMachineType=t2a-standard-2",
              "--experiments=use_runner_v2"
            },
            new String[] {
              "--jobName=v4-t2d-java21",
              "--workerMachineType=t2d-standard-2",
              "--experiments=use_runner_v2"
            });
    final List<BenchFooJob> benchFooJobs =
        jobParams
            .map(args -> create(ObjectArrays.concat(baseArgs, args, String.class)))
            .collect(Collectors.toList());
    benchFooJobs.stream().forEach(BenchFooJob::run);
    benchFooJobs.stream().forEach(BenchFooJob::waitUntilFinish);
    // --runner=DataflowRunner --project=spotify-dbeam --region=europe-west1 --executions=100000
    // --numWorkers=10 --workerMachineType=n2d-highmem-2 --experiments=use_runner_v2
  }

  private static class PT1 extends DoFn<Integer, String> {

    private static final HashFunction hashFunction = Hashing.sha512();
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final String key = "aesEncryptionKey";
    private static final String initVector = "encryptionIntVec";
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";

    private volatile String blackhole;
    private volatile Queue<String> blackholeFifo;
    private final int loops;

    private PT1(final int loops) {
      this.loops = loops;
      this.blackholeFifo = EvictingQueue.create(loops * 10);
    }

    public static String encrypt(String value) {
      try {
        IvParameterSpec iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
        SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

        byte[] encrypted = cipher.doFinal(value.getBytes());
        return Base64.getEncoder().encodeToString(encrypted);
      } catch (Exception ex) {
        LOG.error("Failed to encrypt", ex);
      }
      return null;
    }

    public static String decrypt(String encrypted) {
      try {
        IvParameterSpec iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
        SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
        byte[] original = cipher.doFinal(Base64.getDecoder().decode(encrypted));

        return new String(original);
      } catch (Exception ex) {
        LOG.error("Failed to decrypt", ex);
      }

      return null;
    }

    @ProcessElement
    public void processElement(@Element Integer v, OutputReceiver<String> out) {
      final long start = System.nanoTime();
      blackhole = "";
      IntStream.range(0, loops)
          .mapToObj(i -> heavyFn(10000000L * i + v))
          .collect(Collectors.toList())
          .stream()
          .forEach(
              s -> {
                blackhole = s;
                blackholeFifo.add(s);
                out.output(s);
              });
      final long durationMs = (System.nanoTime() - start) / 1000000L;
      totalProcessElementMs.inc(durationMs);
      processElementMs.update(durationMs);
    }

    private String heavyFn(final long i) {
      return decrypt(
          encrypt(
              encoder.encodeToString(
                  hashFunction
                      .hashUnencodedChars(blackhole + String.format("%10d", i))
                      .asBytes())));
    }

    @Setup
    public void setup() {
      this.blackholeFifo = EvictingQueue.create(loops * 10);
      LOG.info("BenchFooJob setup");
      LOG.info("PIPELINE_OPTIONS: {}", System.getenv("PIPELINE_OPTIONS"));
      LOG.info("RUNNER_CAPABILITIES: {}", System.getenv("RUNNER_CAPABILITIES"));
      // https://issues.apache.org/jira/browse/BEAM-10222
      LOG.info("availableProcessors: {}", Runtime.getRuntime().availableProcessors());
      LOG.info("maxMemory: {}", Runtime.getRuntime().maxMemory());
      // https://issues.apache.org/jira/browse/BEAM-13073
      try {
        Object flags =
            ManagementFactory.getPlatformMBeanServer()
                .invoke(
                    ObjectName.getInstance("com.sun.management:type=DiagnosticCommand"),
                    "vmFlags",
                    new Object[] {null},
                    new String[] {"[Ljava.lang.String;"});
        for (String f : ((String) flags).split("\\s+")) {
          LOG.info(f);
        }
      } catch (Exception e) {
        LOG.error("failed", e);
      }
      final List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
        // print GC info
        // The purpouse of this to troubleshoot that Dataflow runtime is not using suboptimal
        // SerialGC
        LOG.info("{} : {}", gcMxBean.getName(), gcMxBean.getObjectName());
      }
    }
  }
}
