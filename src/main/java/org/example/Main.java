package org.example;

import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class Main {
    public void testCase() throws Exception {
        String value = "a".repeat(2 * 1024 * 1024 + 10);

        System.out.println("Basic line too long");
        Path case1 = Paths.get("line_too_long.csv");
        try {
            writeCsv(case1, value);
            runCopy(case1);
        } finally {
           case1.toFile().delete();
        }

        System.out.println("Basic line too long, with newline");
        Path case2 = Paths.get("line_too_long_with_newline.csv");
        try {
            writeCsv(case2, value + "\n");
            runCopy(case2);
        } finally {
           case2.toFile().delete();
        }

        System.out.println("Multiple lines too long");
        Path case3 = Paths.get("multiple_line_too_long.csv");
        try {
            String line = value + "\n";
            writeCsv(case3, line + line + line + line);
            runCopy(case3);
        } finally {
           case3.toFile().delete();
        }
    }

    public static void writeCsv(Path file, String data) throws IOException {
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(Files.newOutputStream(file))) {
            gzipOutputStream.write(data.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void runCopy(Path csv) throws Exception {
        List<Boolean> parallel = List.of(false, true);
        try (DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE T1 (name VARCHAR)");
            }
            for (boolean p : parallel) {
                System.out.println("  Parallel = " + p);
                try (Statement s = connection.createStatement()) {
                    s.execute("COPY T1(name) from '" + csv + "' (DELIMITER ',', HEADER, COMPRESSION gzip, ALLOW_QUOTED_NULLS false, PARALLEL " + p + ")");
                    System.out.println("    Successful copy.");
                } catch (SQLException e) {
                    System.out.println("    Got Exception: " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.testCase();
    }
}