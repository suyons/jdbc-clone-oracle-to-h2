# JDBC: Clone Oracle to H2

This project is a Java application that clones (migrates) tables and data from an Oracle database to an H2 database using JDBC.  
It reads table structures and data from Oracle and creates equivalent tables and inserts data into H2.

## Features

- Clone table structures and data from Oracle to H2
- Automatic migration using JDBC
- Logging with SLF4J
- Simple configuration and execution

## Requirements

- Java 11 or higher
- Maven 3.x
- Oracle JDBC driver
- H2 Database

## Setup & Usage

1. **Configure Database Connections**  
   Set your Oracle and H2 connection info in the `.env` file:

2. **Run the Application**  
   Use Maven to run the application:

```bash
mvn exec:java -Dexec.mainClass="com.suyons.Main"
```
