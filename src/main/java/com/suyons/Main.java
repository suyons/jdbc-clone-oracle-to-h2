package com.suyons;

import com.suyons.service.OracleBackupService;

public class Main {
    public static void main(String[] args) {
        OracleBackupService oracleBackupService = new OracleBackupService();

        oracleBackupService.printDbUrl();

    }
}
