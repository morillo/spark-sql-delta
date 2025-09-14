# Spark Delta Lake Example

A complete Scala Maven project demonstrating Apache Spark with Delta Lake integration, compatible with IntelliJ IDEA on macOS.

## Project Structure

```
spark-sql-delta/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/morillo/spark/delta/
│   │   │       ├── model/
│   │   │       │   └── User.scala
│   │   │       ├── ConfigManager.scala
│   │   │       ├── DeltaLakeApplication.scala
│   │   │       ├── DeltaTableOperations.scala
│   │   │       └── SparkSessionFactory.scala
│   │   └── resources/
│   │       ├── application.conf
│   │       ├── application-prod.conf
│   │       ├── logback.xml
│   │       └── sample_users.json
│   └── test/
│       └── scala/
│           └── com/morillo/spark/delta/
│               └── DeltaTableOperationsTest.scala
├── .idea/
│   └── runConfigurations/
├── pom.xml
├── .gitignore
└── README.md
```

## Technologies Used

- **Scala**: 2.12.18
- **Apache Spark**: 4.0.1
- **Delta Lake**: 3.2.1
- **Maven**: Build automation
- **ScalaTest**: Testing framework
- **Typesafe Config**: Configuration management
- **Logback**: Logging

## Prerequisites

- **Java**: OpenJDK 17 or higher
- **Maven**: 3.6 or higher
- **IntelliJ IDEA**: 2023.1 or higher with Scala plugin
- **macOS**: Compatible with macOS Monterey and later

## Setup Instructions

### 1. Clone or Download the Project

```bash
git clone <repository-url>
cd spark-sql-delta
```

### 2. Open in IntelliJ IDEA

1. Launch IntelliJ IDEA
2. Choose "Open" and select the project directory
3. IntelliJ will automatically detect the Maven project
4. Wait for Maven dependencies to download

### 3. Import Maven Project

If not automatically imported:

1. Go to **File** → **Open**
2. Select the `pom.xml` file
3. Choose "Open as Project"
4. Select "Import Maven project automatically"

### 4. Configure Scala SDK

1. Go to **File** → **Project Structure** → **Global Libraries**
2. Add Scala SDK 2.12.x if not present
3. Ensure the project modules use the correct Scala SDK

## Running the Application

### From IntelliJ IDEA

The project includes pre-configured run configurations:

1. **DeltaLakeApplication**: Runs the main application
2. **DeltaTableOperationsTest**: Runs the test suite
3. **Maven Package**: Builds the JAR file

To run:
1. Go to **Run** → **Edit Configurations**
2. Select the desired configuration
3. Click **Run**

### From Command Line

#### Compile the project:
```bash
mvn clean compile
```

#### Run tests:
```bash
mvn test
```

#### Package the application:
```bash
mvn clean package
```

#### Run with spark-submit:
```bash
# After packaging
spark-submit \
  --class com.morillo.spark.delta.DeltaLakeApplication \
  --master local[*] \
  target/spark-delta-example-1.0.0.jar
```

## Configuration

### Development (Default)

Uses `application.conf`:
- Local Spark master
- Local file system for Delta tables
- Warehouse directory: `./spark-warehouse`
- Delta table path: `./delta-table`

### Production

Set environment variable and uses `application-prod.conf`:
```bash
export ENVIRONMENT=prod
```

- YARN cluster mode
- HDFS paths for tables
- Optimized Spark settings

## Features Demonstrated

### Core Delta Lake Operations

1. **Table Creation**: Initialize Delta tables with schema
2. **CRUD Operations**: Create, Read, Update, Delete operations
3. **Merge Operations**: Upsert functionality
4. **Time Travel**: Query historical versions of data
5. **Table Utilities**: History, details, vacuum operations

### Sample Operations

The application demonstrates:

- Creating a Delta table with user data
- Inserting sample user records
- Querying users by department
- Finding high-earning employees
- Updating salary information
- Merging new user data
- Time travel queries
- Table maintenance operations

### Data Model

```scala
case class User(
  id: Long,
  name: String,
  email: String,
  age: Int,
  department: String,
  salary: Double,
  created_at: Timestamp,
  updated_at: Option[Timestamp] = None
)
```

## Testing

The project includes comprehensive unit tests using ScalaTest:

```bash
mvn test
```

Tests cover:
- Basic CRUD operations
- Data filtering and aggregation
- Update and merge operations
- Time travel functionality
- Table maintenance operations

## Building for Distribution

### Create JAR with Dependencies

```bash
mvn clean package
```

This creates:
- `target/spark-delta-example-1.0.0.jar`: Fat JAR with all dependencies
- `target/original-spark-delta-example-1.0.0.jar`: Original JAR without dependencies

### Running the JAR

#### Local Mode
```bash
java -cp target/spark-delta-example-1.0.0.jar com.morillo.spark.delta.DeltaLakeApplication
```

#### With spark-submit
```bash
spark-submit \
  --class com.morillo.spark.delta.DeltaLakeApplication \
  --master local[4] \
  --deploy-mode client \
  target/spark-delta-example-1.0.0.jar
```

#### Cluster Mode
```bash
spark-submit \
  --class com.morillo.spark.delta.DeltaLakeApplication \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --driver-memory 2g \
  target/spark-delta-example-1.0.0.jar
```

## Troubleshooting

### Common Issues

1. **Java Version Issues**
   - Ensure OpenJDK 17 or higher is installed
   - Check `JAVA_HOME` environment variable
   - On macOS with Homebrew: `brew install openjdk@17`

2. **Scala Version Mismatch**
   - Verify Scala 2.12.x is configured in IntelliJ
   - Check Maven dependencies for version conflicts

3. **Memory Issues**
   - Increase JVM heap size: `-Xmx4g`
   - Adjust Spark executor memory settings

4. **Delta Table Location**
   - Ensure write permissions for table directories
   - Check path configurations in `application.conf`

### Logging

Logs are configured via `logback.xml`:
- Console output with INFO level
- File output to `logs/spark-delta-app.log`
- Reduced Spark framework logging

## Development Tips

### IntelliJ IDEA Setup

1. **Enable Scala Plugin**: Ensure the Scala plugin is installed and enabled
2. **Code Style**: Import Scala code style settings
3. **Run Configurations**: Use the provided run configurations for easy development
4. **Maven Integration**: Enable auto-import for Maven dependencies

### Performance Tuning

For local development:
```scala
.config("spark.sql.shuffle.partitions", "4")
.config("spark.default.parallelism", "4")
```

For production:
```scala
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
```

## License

This project is provided as an example for educational and development purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Spark and Delta Lake documentation
3. Create an issue in the project repository