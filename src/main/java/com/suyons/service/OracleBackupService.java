package com.suyons.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@Getter
public class OracleBackupService {
    private static final Dotenv dotenv = Dotenv.load();

    private static final String ORACLE_URL = dotenv.get("ORACLE_URL");
    private static final String ORACLE_USERNAME = dotenv.get("ORACLE_USERNAME");
    private static final String ORACLE_PASSWORD = dotenv.get("ORACLE_PASSWORD");
    private static final String H2_URL = dotenv.get("H2_URL");
    private static final String H2_USERNAME = dotenv.get("H2_USERNAME");
    private static final String H2_PASSWORD = dotenv.get("H2_PASSWORD");

    // Oracle schemas to exclude from backup (e.g., system schemas)
    private static final List<String> EXCLUDE_SCHEMAS = Arrays.asList(
            "ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSFWUSER", "DBSNMP", "DIP", "DVF", "DVSYS",
            "GGSYS", "GSMADMIN_INTERNAL", "GSMCATUSER", "GSMROOTUSER", "GSMUSER", "HR", "LBACSYS",
            "MDDATA", "MDSYS", "OJVMSYS", "OLAPSYS", "ORACLE_OCM", "ORDDATA", "ORDPLUGINS", "ORDSYS",
            "OUTLN", "REMOTE_SCHEDULER_AGENT", "SI_INFORMTN_SCHEMA", "SYS", "SYS$UMF", "SYSBACKUP",
            "SYSDG", "SYSKM", "SYSRAC", "SYSTEM", "WMSYS", "XDB", "XS$NULL");

    public void execute() {
        log.info("Starting Oracle to H2 backup.");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            log.error("JDBC driver not found: {}", e.getMessage(), e);
            return;
        }

        try (Connection oracleConn = DriverManager.getConnection(ORACLE_URL, ORACLE_USERNAME, ORACLE_PASSWORD);
                Connection h2Conn = DriverManager.getConnection(H2_URL, H2_USERNAME, H2_PASSWORD)) {

            // Query schema list from Oracle and create schemas in H2, excluding
            // EXCLUDE_SCHEMAS
            List<String> schemaNames = new ArrayList<>();
            StringBuilder excludeClause = new StringBuilder();
            for (int i = 0; i < EXCLUDE_SCHEMAS.size(); i++) {
                excludeClause.append("?");
                if (i < EXCLUDE_SCHEMAS.size() - 1) {
                    excludeClause.append(", ");
                }
            }
            String schemaSql = "SELECT USERNAME FROM ALL_USERS WHERE USERNAME NOT IN (" + excludeClause + ")";
            try (PreparedStatement pstmt = oracleConn.prepareStatement(schemaSql)) {
                for (int i = 0; i < EXCLUDE_SCHEMAS.size(); i++) {
                    pstmt.setString(i + 1, EXCLUDE_SCHEMAS.get(i));
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                }
            }
            log.info("Schemas to create (total {}): {}", schemaNames.size(), schemaNames);
            try (Statement h2Stmt = h2Conn.createStatement()) {
                for (String schema : schemaNames) {
                    h2Stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schema);
                    log.info("H2 schema '{}' created.", schema);
                }
            }

            log.info("Starting H2 user creation...");
            try (Statement h2Stmt = h2Conn.createStatement()) {
                for (String schema : schemaNames) {
                    // H2 CREATE USER throws exception if user exists, so ignore exceptions
                    try {
                        h2Stmt.executeUpdate("CREATE USER IF NOT EXISTS " + schema + " PASSWORD '" + H2_PASSWORD + "'");
                        log.info("H2 user '{}' created.", schema);
                    } catch (SQLException e) {
                        log.warn("Exception while creating H2 user '{}' (may already exist): {}", schema,
                                e.getMessage());
                    }
                }
            }

            log.info("Granting privileges to H2 users...");
            try (Statement h2Stmt = h2Conn.createStatement()) {
                for (String schema : schemaNames) {
                    try {
                        h2Stmt.executeUpdate("GRANT ALL ON SCHEMA " + schema + " TO " + schema);
                        log.info("Granted all privileges on schema '{}' to user '{}'.", schema, schema);
                    } catch (SQLException e) {
                        log.warn("Exception while granting privileges to user '{}': {}", schema, e.getMessage());
                    }
                }
            }

            h2Conn.setAutoCommit(false); // Manual transaction management for H2

            List<String> tableNames = getAllUserTables(oracleConn);
            log.info("Tables to backup (total {}): {}", tableNames.size(), tableNames);

            for (String tableName : tableNames) {
                try {
                    backupTable(oracleConn, h2Conn, tableName);
                    h2Conn.commit(); // Commit after each table backup
                } catch (SQLException e) {
                    h2Conn.rollback(); // Rollback on error
                    log.error("Error while backing up table '{}': {}", tableName, e.getMessage(), e);
                }
            }
            log.info("All table backup processes completed.");

        } catch (SQLException e) {
            log.error("Database connection or initialization error: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error occurred: {}", e.getMessage(), e);
        }
    }

    /**
     * Retrieves the list of user tables to backup from Oracle.
     */
    private List<String> getAllUserTables(Connection oracleConn) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        // Use ALL_TABLES view to get all accessible tables for the current user.
        // Add OWNER filter to exclude system tables.
        String sql = "SELECT OWNER || '.' || TABLE_NAME FROM ALL_TABLES " +
                "WHERE OWNER NOT IN (?) " +
                "AND TABLE_NAME NOT LIKE 'BIN$%'"; // Exclude recycle bin objects

        // Dynamically bind values for IN clause in PreparedStatement
        StringBuilder ownerInClause = new StringBuilder();
        for (int i = 0; i < EXCLUDE_SCHEMAS.size(); i++) {
            ownerInClause.append("?");
            if (i < EXCLUDE_SCHEMAS.size() - 1) {
                ownerInClause.append(", ");
            }
        }
        sql = "SELECT OWNER || '.' || TABLE_NAME FROM ALL_TABLES WHERE OWNER NOT IN (" + ownerInClause.toString()
                + ") AND TABLE_NAME NOT LIKE 'BIN$%' AND NESTED='NO'";

        try (PreparedStatement pstmt = oracleConn.prepareStatement(sql)) {
            for (int i = 0; i < EXCLUDE_SCHEMAS.size(); i++) {
                pstmt.setString(i + 1, EXCLUDE_SCHEMAS.get(i));
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    tableNames.add(rs.getString(1));
                }
            }
        }
        return tableNames;
    }

    /**
     * Backs up a single table from Oracle to H2.
     */
    private void backupTable(Connection oracleConn, Connection h2Conn, String fullTableName) throws SQLException {
        log.info("Starting backup for table '{}'.", fullTableName);
        Statement oracleStmt = null;
        PreparedStatement h2Pstmt = null;
        ResultSet rs = null;

        try {
            // 1. Drop and create table in H2 (full backup each time)
            // For production, consider incremental backup logic or MERGE/UPSERT for
            // existing tables
            try (Statement stmt = h2Conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + fullTableName);
                log.debug("Dropped H2 table '{}'.", fullTableName);
            }

            oracleStmt = oracleConn.createStatement();
            rs = oracleStmt.executeQuery("SELECT * FROM " + fullTableName);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Dynamically generate H2 CREATE TABLE SQL
            StringBuilder createTableSql = new StringBuilder("CREATE TABLE " + fullTableName + " (");
            List<String> columnNames = new ArrayList<>();
            List<Integer> columnTypes = new ArrayList<>(); // JDBC SQL Types

            for (int i = 1; i <= columnCount; i++) {
                String colName = metaData.getColumnName(i);
                int jdbcSqlType = metaData.getColumnType(i); // JDBC SQL Type
                int precision = metaData.getPrecision(i);
                int scale = metaData.getScale(i);

                columnNames.add(colName);
                columnTypes.add(jdbcSqlType);

                // Map Oracle data types to H2-compatible types
                String h2ColType = mapOracleTypeToH2Type(jdbcSqlType, precision, scale,
                        metaData.getColumnDisplaySize(i));
                createTableSql.append(colName).append(" ").append(h2ColType);
                if (i < columnCount) {
                    createTableSql.append(", ");
                }
            }
            createTableSql.append(")");

            try (Statement stmt = h2Conn.createStatement()) {
                stmt.executeUpdate(createTableSql.toString());
                log.info("Created H2 table '{}'.", fullTableName);
            }

            // 2. Read data from Oracle and insert into H2
            StringBuilder insertSql = new StringBuilder("INSERT INTO " + fullTableName + " (");
            insertSql.append(String.join(", ", columnNames)).append(") VALUES (");
            for (int i = 0; i < columnCount; i++) {
                insertSql.append("?");
                if (i < columnCount - 1) {
                    insertSql.append(", ");
                }
            }
            insertSql.append(")");

            h2Pstmt = h2Conn.prepareStatement(insertSql.toString());
            int rowCount = 0;
            final int BATCH_SIZE = 5000; // Batch insert size

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    // PreparedStatement.setObject() handles JDBC type mapping.
                    // For certain Oracle types not recognized by H2, explicit conversion may be
                    // needed.
                    Object value = rs.getObject(i);
                    // Handle LOB types (e.g., convert to String or byte[])
                    if (value instanceof java.sql.Clob) {
                        java.sql.Clob clob = (java.sql.Clob) value;
                        value = clob.getSubString(1, (int) clob.length());
                    } else if (value instanceof java.sql.Blob) {
                        java.sql.Blob blob = (java.sql.Blob) value;
                        value = blob.getBytes(1, (int) blob.length());
                    }
                    h2Pstmt.setObject(i, value);
                }
                h2Pstmt.addBatch();
                rowCount++;

                if (rowCount % BATCH_SIZE == 0) {
                    h2Pstmt.executeBatch();
                    h2Pstmt.clearBatch();
                    log.info("Processed {} records from table '{}'.", rowCount, fullTableName);
                }
            }
            h2Pstmt.executeBatch(); // Execute remaining batch
            log.info("Backed up {} records to table '{}'.", rowCount, fullTableName);

        } finally {
            // Release resources
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    log.warn("Error closing ResultSet: {}", e.getMessage());
                }
            if (oracleStmt != null)
                try {
                    oracleStmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing Oracle Statement: {}", e.getMessage());
                }
            if (h2Pstmt != null)
                try {
                    h2Pstmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing H2 PreparedStatement: {}", e.getMessage());
                }
        }
    }

    /**
     * Returns a data type string suitable for H2 based on Oracle JDBC type and
     * metadata.
     * For perfect mapping, refer to H2 documentation or test thoroughly.
     */
    private String mapOracleTypeToH2Type(int jdbcSqlType, int precision, int scale, int displaySize) {
        switch (jdbcSqlType) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return "VARCHAR(" + Math.min(displaySize, 4000) + ")";
            case Types.CHAR:
            case Types.NCHAR:
                return "CHAR(" + displaySize + ")";
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (scale > 0) {
                    return "DECIMAL(" + precision + ", " + scale + ")";
                } else if (precision > 0 && precision <= 9) {
                    return "INT";
                } else if (precision > 9 && precision <= 18) {
                    return "BIGINT";
                } else {
                    return "DECIMAL(" + precision + ", " + scale + ")";
                }
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.REAL:
                return "REAL";
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "TIMESTAMP";
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
            case Types.NCLOB:
                return "CLOB";
            case Types.BOOLEAN:
                return "BOOLEAN";
            // Add more mappings for Oracle-specific types if needed
            default:
                log.warn("Unknown JDBC type mapping: {} (precision: {}, scale: {}, displaySize: {})", jdbcSqlType,
                        precision,
                        scale, displaySize);
                return "VARCHAR(MAX)";
        }
    }
}
