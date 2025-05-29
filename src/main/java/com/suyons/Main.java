package com.suyons;

import com.suyons.service.OracleBackupService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        OracleBackupService oracleBackupService = new OracleBackupService();
        oracleBackupService.execute();
    }
}
