package com.suyons.service;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class OracleBackupService {
    private static final Dotenv dotenv = Dotenv.load();

    public void printDbUrl() {
        String dbUrl = dotenv.get("ORACLE_URL");
        log.info("ORACLE_URL: {}", dbUrl);
    }
}
