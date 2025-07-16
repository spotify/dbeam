# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DBeam is a mature, production-ready data extraction tool developed by Spotify that exports data from SQL databases to Google Cloud Storage in Avro format using Apache Beam pipelines. The project has been in production since August 2017 and is considered a "finished product" requiring minimal ongoing development.

### Core Purpose
- Extract complete tables from SQL databases (PostgreSQL, MySQL, MariaDB, H2)
- Convert data to Avro format with automatic schema inference
- Store results in cloud storage (primarily GCS)
- Support both single-threaded and parallel query execution
- Integrate with Google Cloud Dataflow for scalable processing

## Architecture Overview

### Technology Stack
- **Language**: Java 8+ (supports Java 11, 17, 21)
- **Build System**: Maven with multi-module structure
- **Core Framework**: Apache Beam SDK 2.65.0
- **Data Format**: Apache Avro 1.11.4
- **Database Connectivity**: JDBC with vendor-specific drivers
- **Cloud Integration**: Google Cloud SDK, KMS encryption support

### Project Structure

```
dbeam/
├── dbeam-core/           # Main implementation module
│   ├── src/main/java/com/spotify/dbeam/
│   │   ├── args/         # Query building and connection utilities
│   │   ├── avro/         # Avro schema generation and I/O
│   │   ├── beam/         # Apache Beam pipeline helpers
│   │   ├── jobs/         # Main job classes and entry points
│   │   └── options/      # Pipeline configuration and options
│   └── src/test/         # Comprehensive test suite
├── dbeam-bom/           # Bill of Materials for dependency management
├── docs/                # Documentation (type conversion details)
├── e2e/                 # End-to-end testing with Docker PostgreSQL
└── .github/             # CI/CD workflows and configurations
```

## Key Components

### Main Entry Points
- **`JdbcAvroJob`**: Primary job class for database-to-Avro exports
- **`PsqlAvroJob`**: PostgreSQL-specific optimizations
- **`BenchJdbcAvroJob`**: Performance benchmarking variant

### Core Architecture Packages

#### Args Package (`com.spotify.dbeam.args`)
- **`JdbcExportArgs`**: Configuration container for export parameters
- **`QueryBuilder`**: SQL query construction for data extraction
- **`ParallelQueryBuilder`**: Multi-query generation for parallel processing
- **`JdbcConnectionUtil`**: Database connection management and validation

#### Avro Package (`com.spotify.dbeam.avro`)
- **`JdbcAvroSchema`**: Automatic Avro schema generation from JDBC metadata
- **`JdbcAvroIO`**: Beam I/O transforms for reading/writing Avro
- **`JdbcAvroRecord`**: Data conversion from JDBC ResultSet to Avro
- **`BeamJdbcAvroSchema`**: Beam-specific schema handling

#### Options Package (`com.spotify.dbeam.options`)
- **`DBeamPipelineOptions`**: Core pipeline configuration interface
- **`JdbcExportPipelineOptions`**: Database export-specific options
- **`OutputOptions`**: Output location and format configuration
- **`PasswordReader`**: Secure password handling with KMS support

## Development Commands

### Build Commands
```bash
# Standard build and test
mvn verify

# Build with test coverage
mvn verify -Pcoverage

# Create shaded JAR with all dependencies
mvn clean package -Ppack

# Run single test
mvn test -Dtest=ClassName#methodName

# Check dependencies for updates
mvn versions:display-dependency-updates
```

### Testing Strategy
- **Unit Tests**: Comprehensive coverage with H2 in-memory database
- **Integration Tests**: Docker-based PostgreSQL testing via e2e scripts
- **Coverage Target**: 60% minimum instruction coverage (Jacoco)
- **Quality Gates**: Checkstyle with Google Java style enforcement

### Release Process
- Uses GitHub Actions with Sonatype deployment
- Maven release plugin with automatic version management
- GPG signing and credential management via GitHub secrets
- Releases are published to Maven Central

## Architectural Patterns

### Builder Pattern
Extensive use for query construction and argument building:
```java
QueryBuilder.create(connectionArgs)
    .withTable(tableName)
    .withPartitionColumn(partitionColumn)
    .build()
```

### Options Pattern
Beam-style configuration management with interface inheritance:
```java
public interface JdbcExportPipelineOptions extends PipelineOptions {
    @Description("The database table to query")
    String getTable();
    void setTable(String table);
}
```

### Transform Pattern
Beam pipeline transforms for data processing:
```java
pipeline
    .apply("Read from Database", JdbcAvroIO.createRead(exportArgs))
    .apply("Write to Storage", JdbcAvroIO.createWrite(outputPath))
```

## Configuration and Security

### Pipeline Options
- Interface-based configuration with `@Description` annotations
- Automatic command-line parsing and validation
- Hierarchical options inheritance

### Security Features
- **Password Management**: File-based or KMS-encrypted password storage
- **IAM Authentication**: Google Cloud IAM integration for CloudSQL
- **Connection Security**: SSL/TLS support for database connections

### Performance Optimizations
- **Parallel Queries**: Split large tables using numeric columns with `--queryParallelism`
- **Fetch Size Configuration**: Optimized JDBC result set fetching with `--fetchSize`
- **Beam Runners**: Support for DirectRunner (local) and DataflowRunner (cloud)

## Database Support

### Supported Databases
- **PostgreSQL**: Full support with CloudSQL integration
- **MySQL**: Including CloudSQL with specific connector optimizations
- **MariaDB**: Complete compatibility
- **H2**: Primarily for testing scenarios

### Type Conversion
- Automatic JDBC to Avro type mapping
- Logical type support for complex data types
- Comprehensive type conversion documentation in `docs/`

## Code Quality Standards

### Style and Formatting
- **Google Java Style**: Enforced via Checkstyle with minimal suppressions
- **Documentation**: Focused on public APIs and complex algorithms
- **Error Handling**: Comprehensive with meaningful error messages

### Dependency Management
- Managed via parent POM with careful version alignment
- Apache Beam BOM for framework consistency
- Google Cloud Libraries BOM for cloud integration
- Explicit version overrides for security and compatibility

## Production Considerations

### Performance Tuning
- Match `--queryParallelism` to vCPU count for optimal performance
- Use `--splitColumn` for parallel execution on large tables
- Tune `--fetchSize` based on memory constraints

### Monitoring
- Built-in Beam metrics collection via `MetricsHelper`
- SLF4J logging with configurable levels
- Output validation with configurable minimum row count

### Operational Patterns
- Incremental exports using `--partitionColumn` for date-based filtering
- Configurable export timeouts to prevent hanging jobs
- Comprehensive exception handling with clear error messages