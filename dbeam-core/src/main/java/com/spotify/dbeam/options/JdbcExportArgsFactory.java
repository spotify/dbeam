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

package com.spotify.dbeam.options;

import static com.google.common.base.Preconditions.checkArgument;

import com.spotify.dbeam.args.JdbcAvroArgs;
import com.spotify.dbeam.args.JdbcConnectionArgs;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.args.QueryBuilderArgs;
import com.spotify.dbeam.avro.BeamJdbcAvroSchema;
import com.spotify.dbeam.beam.BeamHelper;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.PipelineOptions;

public class JdbcExportArgsFactory {

  private static DateTimeFormatter INSTANT_PARSER_WITH_ZONE =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendPattern("yyyy[-MM][-dd['T'HH[:mm[:ss]]]]")
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .optionalStart() // optionally support offset id
          .appendOffsetId()
          .toFormatter()
          .withZone(ZoneId.of("UTC"));

  public static JdbcExportArgs fromPipelineOptions(final PipelineOptions options)
      throws ClassNotFoundException, IOException {
    final JdbcExportPipelineOptions exportOptions = options.as(JdbcExportPipelineOptions.class);
    final boolean useLogicalTypes = resolveUseLogicalTypes(exportOptions);
    checkArgument(
        !exportOptions.isUseTimestampMicros() || useLogicalTypes,
        "--useTimestampMicros requires --useLogicalTypes (or its deprecated alias "
            + "--useAvroLogicalTypes)");
    final JdbcAvroArgs jdbcAvroArgs =
        JdbcAvroArgs.create(
            JdbcConnectionArgs.create(exportOptions.getConnectionUrl())
                .withUsername(exportOptions.getUsername())
                .withPassword(PasswordReader.INSTANCE.readPassword(exportOptions).orElse(null)),
            exportOptions.getFetchSize(),
            exportOptions.getAvroCodec(),
            Optional.ofNullable(exportOptions.getPreCommand()).orElse(Collections.emptyList()),
            ArrayHandlingMode.validateValue(exportOptions.getArrayMode()),
            exportOptions.isNullableArrayItems(),
            exportOptions.isUseTimestampMicros()
            );

    final Optional<Schema> inputAvroSchema =
        BeamJdbcAvroSchema.parseOptionalInputAvroSchemaFile(exportOptions.getAvroSchemaFilePath());
    inputAvroSchema.ifPresent(
        schema -> validateInputSchemaTimestampUnits(schema, exportOptions.isUseTimestampMicros()));

    return JdbcExportArgs.create(
        jdbcAvroArgs,
        createQueryArgs(exportOptions),
        exportOptions.getAvroSchemaNamespace(),
        Optional.ofNullable(exportOptions.getAvroSchemaName()),
        Optional.ofNullable(exportOptions.getAvroDoc()),
        useLogicalTypes,
        exportOptions.isUseTimestampMicros(),
        Duration.parse(exportOptions.getExportTimeout()),
        inputAvroSchema);
  }

  /**
   * {@code --useTimestampMicros} alone decides whether timestamp/date/time columns are written as
   * epoch millis or micros, while a {@code --avroSchemaFilePath} schema is used verbatim as the
   * writer schema. Reject a schema file whose timestamp logical types declare a different unit, so
   * readers never misinterpret the values. Plain {@code long} fields are accepted.
   *
   * @throws IllegalArgumentException naming every offending field
   */
  public static void validateInputSchemaTimestampUnits(
      final Schema schema, final boolean useTimestampMicros) {
    final List<String> errors = new ArrayList<>();
    collectTimestampUnitErrors(schema, "", useTimestampMicros, errors, new HashSet<>());
    checkArgument(
        errors.isEmpty(),
        "--avroSchemaFilePath schema declares timestamp units that do not match "
            + "--useTimestampMicros (%s). Values are written as epoch %s. Either %s the flag or "
            + "change the schema to use %s (or plain long). Problems: %s",
        useTimestampMicros ? "set" : "not set",
        useTimestampMicros ? "micros" : "millis",
        useTimestampMicros ? "remove" : "set",
        useTimestampMicros ? "timestamp-micros" : "timestamp-millis",
        String.join("; ", errors));
  }

  private static void collectTimestampUnitErrors(
      final Schema schema,
      final String path,
      final boolean useTimestampMicros,
      final List<String> errors,
      final Set<String> visitedRecords) {
    switch (schema.getType()) {
      case RECORD:
        if (!visitedRecords.add(schema.getFullName())) {
          return;
        }
        for (Schema.Field field : schema.getFields()) {
          final String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
          collectTimestampUnitErrors(
              field.schema(), fieldPath, useTimestampMicros, errors, visitedRecords);
        }
        return;
      case UNION:
        for (Schema branch : schema.getTypes()) {
          collectTimestampUnitErrors(branch, path, useTimestampMicros, errors, visitedRecords);
        }
        return;
      case ARRAY:
        collectTimestampUnitErrors(
            schema.getElementType(), path + "[]", useTimestampMicros, errors, visitedRecords);
        return;
      case MAP:
        collectTimestampUnitErrors(
            schema.getValueType(), path + "{}", useTimestampMicros, errors, visitedRecords);
        return;
      default:
        final String logicalType = schema.getProp("logicalType");
        if (logicalType == null) {
          return;
        }
        final String expected = useTimestampMicros ? "timestamp-micros" : "timestamp-millis";
        final boolean isTimestamp =
            logicalType.startsWith("timestamp-") || logicalType.startsWith("local-timestamp-");
        if (isTimestamp && !logicalType.equals(expected)) {
          errors.add(String.format("%s has logicalType %s, expected %s", path, logicalType,
              expected));
        }
    }
  }

  /**
   * Resolves the effective value of {@code --useLogicalTypes}, honoring the deprecated
   * {@code --useAvroLogicalTypes} alias. Fails if both are set to different values.
   */
  @SuppressWarnings("deprecation")
  static boolean resolveUseLogicalTypes(final JdbcExportPipelineOptions options) {
    final Boolean useLogicalTypes = options.isUseLogicalTypes();
    final Boolean useAvroLogicalTypes = options.isUseAvroLogicalTypes();
    if (useLogicalTypes != null && useAvroLogicalTypes != null) {
      checkArgument(
          useLogicalTypes.equals(useAvroLogicalTypes),
          "Conflicting values: --useLogicalTypes=%s and --useAvroLogicalTypes=%s. "
              + "--useAvroLogicalTypes is a deprecated alias of --useLogicalTypes; "
              + "set only --useLogicalTypes.",
          useLogicalTypes,
          useAvroLogicalTypes);
      return useLogicalTypes;
    }
    if (useLogicalTypes != null) {
      return useLogicalTypes;
    }
    if (useAvroLogicalTypes != null) {
      return useAvroLogicalTypes;
    }
    return false;
  }

  public static QueryBuilderArgs createQueryArgs(final JdbcExportPipelineOptions options)
      throws IOException {
    final TemporalAmount partitionPeriod =
        partitionPeriodTemporalAmount(options.getPartitionPeriod());
    final Optional<Instant> partition =
        Optional.ofNullable(options.getPartition()).map(JdbcExportArgsFactory::parseInstant);
    final Optional<String> partitionColumn = Optional.ofNullable(options.getPartitionColumn());
    checkArgument(
        !partitionColumn.isPresent() || partition.isPresent(),
        "To use --partitionColumn the --partition parameter must also be configured");

    if (!(options.isSkipPartitionCheck() || partitionColumn.isPresent())) {
      Instant minPartitionDateTime =
          Optional.ofNullable(options.getMinPartitionPeriod())
              .map(JdbcExportArgsFactory::parseInstant)
              // given Instant does not support operations with ChronoUnit.MONTHS
              .orElse(
                  Instant.now()
                      .atOffset(ZoneOffset.UTC)
                      .minus(partitionPeriod) // subtracts two partitions
                      .minus(partitionPeriod)
                      .toInstant());
      partition.map(p -> validatePartition(p, minPartitionDateTime));
    }

    final Optional<String> splitColumn = Optional.ofNullable(options.getSplitColumn());
    final Optional<Integer> queryParallelism = Optional.ofNullable(options.getQueryParallelism());
    checkArgument(
        queryParallelism.isPresent() == splitColumn.isPresent(),
        "Either both --queryParallelism and --splitColumn must be present or " + "none of them");
    queryParallelism.ifPresent(
        p ->
            checkArgument(
                p > 0,
                "Query Parallelism must be a positive number. Specified queryParallelism was %s",
                p));

    return createQueryBuilderArgs(options)
        .builder()
        .setLimit(Optional.ofNullable(options.getLimit()))
        .setPartitionColumn(partitionColumn)
        .setPartition(partition)
        .setPartitionPeriod(partitionPeriod)
        .setSplitColumn(splitColumn)
        .setQueryParallelism(queryParallelism)
        .build();
  }

  /**
   * Defaults to a Period of 1 days in case of empty. If partitionPeriod argument is a sub day unit
   * (e.g. hourly), then uses Duration (stored in seconds). Otherwise use Period.
   *
   * @param partitionPeriod partitionPeriod parameter to be parsed
   * @return A Period or Duration based on partitionPeriod
   */
  private static TemporalAmount partitionPeriodTemporalAmount(final String partitionPeriod) {
    if (partitionPeriod == null) {
      return Period.ofDays(1);
    } else if (partitionPeriod.contains("PT")) {
      return Duration.parse(partitionPeriod);
    } else {
      return Period.parse(partitionPeriod);
    }
  }

  private static QueryBuilderArgs createQueryBuilderArgs(final JdbcExportPipelineOptions options)
      throws IOException {
    checkArgument(
        (options.getTable() != null) != (options.getSqlFile() != null),
        "Either --table or --sqlFile must be present");
    if (options.getSqlFile() != null) {
      return QueryBuilderArgs.createFromQuery(BeamHelper.readFromFile(options.getSqlFile()));
    } else {
      return QueryBuilderArgs.create(options.getTable());
    }
  }

  public static Instant parseInstant(String v) {
    return Instant.from(INSTANT_PARSER_WITH_ZONE.parse(v));
  }

  private static Instant validatePartition(
      final Instant partitionDateTime, final Instant minPartitionDateTime) {
    checkArgument(
        partitionDateTime.isAfter(minPartitionDateTime),
        "Too old partition date %s. Use a partition date >= %s or use --skip-partition-check",
        partitionDateTime,
        minPartitionDateTime);
    return partitionDateTime;
  }
}
