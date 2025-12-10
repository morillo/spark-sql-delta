# Running the Spark Delta Lake Application

## Summary of Changes

### What Was Fixed
The application was failing with a `VersionNotFoundException` error when trying to time travel to version 0 of the Delta table. This was **NOT** related to the macOS Tahoe upgrade, but rather:

1. **Root Cause**: Previous runs of the application created Delta table versions 0-39, which were subsequently cleaned up (VACUUMed)
2. **Only versions 40-43 remained available** when you ran the application
3. **The code was hardcoded** to try reading version 0, which no longer existed

### Code Improvements
Added robust version checking and handling in `DeltaTableOperations.scala`:

1. **`getAvailableVersions()`** - Retrieves all available versions from Delta history
2. **`getEarliestAvailableVersion()`** - Gets the earliest available version
3. **`timeTravelSafe()`** - Safely attempts time travel with availability checking

The application now:
- Automatically reads the earliest available version instead of hardcoding version 0
- Gracefully handles missing versions with warning messages
- Continues execution even when requested versions aren't available

## Running the Application

### Option 1: IntelliJ IDEA (Already Configured)
Your IntelliJ run configuration already has the proper JVM arguments configured. Just click Run.

### Option 2: Command Line with spark-submit

#### Prerequisites
- Spark 3.5.2 installed at: `/usr/local/spark-versions/spark-3.5.2`
- JAR file built: `target/spark-delta-example-1.0.0.jar`

#### Build the JAR
```bash
mvn clean package
```

#### Run with spark-submit
```bash
/usr/local/spark-versions/spark-3.5.2/bin/spark-submit \
  --class com.morillo.spark.delta.DeltaLakeApplication \
  --master "local[*]" \
  --driver-memory 4g \
  --driver-java-options "\
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=java.base/sun.security.action=ALL-UNNAMED \
--add-exports=java.base/sun.util.calendar=ALL-UNNAMED \
--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED" \
  target/spark-delta-example-1.0.0.jar
```

## Important Notes

### Java 17 + Spark 3.5.2 Requirements
The `--add-opens` and `--add-exports` flags are **required** for Spark 3.5.2 to run on Java 17+. This is standard for Spark 3.x on modern Java versions and is not specific to macOS Tahoe.

### Spark Version Compatibility
- **Application**: Uses Spark 3.5.2 (from Maven dependencies)
- **Installed spark-submit**: `/opt/homebrew/bin/spark-submit` is Spark 4.0.1
- **Correct spark-submit**: `/usr/local/spark-versions/spark-3.5.2/bin/spark-submit`

**Always use the Spark 3.5.2 spark-submit** to match your application's Spark version.

### macOS Tahoe (26.1) Compatibility
The application runs perfectly on macOS Tahoe 26.1 with no additional changes needed beyond the version checking improvements.

## Expected Output

When you run the application, you should see:

1. Delta table creation
2. Sample data insertion
3. Query results for all users
4. Query results for Engineering department
5. High earners (salary > 80000)
6. Salary update for Alice
7. Merge operation with new users
8. Table history
9. Table details
10. **Time travel to earliest available version** (version 40 in your case)
11. **Warning message** indicating version 0 is not available

All operations complete successfully without errors.

## Clean Start (Optional)

If you want to start with a fresh Delta table where version 0 will exist:

```bash
rm -rf delta-table
```

Then run the application again - version 0 will be created and accessible.
