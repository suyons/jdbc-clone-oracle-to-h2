package com.suyons;

import com.suyons.service.SelectResultPrinter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        // OracleBackupService oracleBackupService = new OracleBackupService();
        // oracleBackupService.execute();
        // Also run the select-and-export utility (writes select_result.csv)
        SelectResultPrinter printer = new SelectResultPrinter();
        printer.executeAndSaveCsv();
        System.exit(0);
    }
}
