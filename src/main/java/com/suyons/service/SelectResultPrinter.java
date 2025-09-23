package com.suyons.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@Getter
public class SelectResultPrinter {
    private static final Dotenv dotenv = Dotenv.load();

    private static final String ORACLE_URL = dotenv.get("ORACLE_URL");
    private static final String ORACLE_USERNAME = dotenv.get("ORACLE_USERNAME");
    private static final String ORACLE_PASSWORD = dotenv.get("ORACLE_PASSWORD");

    // Load SELECT SQL from resources/input.sql (classpath root)
    private static final String SELECT_SQL = loadSql();

    private static String loadSql() {
        InputStream in = SelectResultPrinter.class.getResourceAsStream("/input.sql");
        if (in == null) {
            log.warn("Resource /input.sql not found on classpath, using default SQL.");
            return "SELECT 1 AS EXAMPLE_VALUE FROM DUAL";
        }
        log.info("Loading SQL from /input.sql");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString().trim();
        } catch (IOException e) {
            log.error("Failed to read /input.sql: {}", e.getMessage(), e);
            return "SELECT 1 AS EXAMPLE_VALUE FROM DUAL";
        }
    }

    /**
     * Executes the SELECT_SQL against Oracle and writes the result as CSV to the
     * given file path.
     */
    public void executeAndSaveCsv() {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String outputFilePath = "output_" + timestamp + ".csv";

        log.info("Connecting to Oracle: {}", ORACLE_URL);
        try (Connection conn = DriverManager.getConnection(ORACLE_URL, ORACLE_USERNAME, ORACLE_PASSWORD);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            // Write header
            for (int i = 1; i <= colCount; i++) {
                writer.write(escapeCsv(meta.getColumnLabel(i)));
                if (i < colCount)
                    writer.write(',');
            }
            writer.newLine();

            // Write rows
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    writer.write(escapeCsv(val == null ? "" : String.valueOf(val)));
                    if (i < colCount)
                        writer.write(',');
                }
                writer.newLine();
            }
            writer.flush();
            log.info("CSV saved to {}", outputFilePath);

        } catch (SQLException e) {
            log.error("SQL error while executing select: {}", e.getMessage(), e);
        } catch (IOException e) {
            log.error("IO error while writing CSV: {}", e.getMessage(), e);
        }
    }

    // Minimal CSV escaping: wrap fields containing comma, quote or newline in
    // quotes and escape internal quotes
    private String escapeCsv(String field) {
        if (field == null)
            return "";
        boolean needQuote = field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r");
        String escaped = field.replace("\"", "\"\"");
        return needQuote ? "\"" + escaped + "\"" : escaped;
    }
}
