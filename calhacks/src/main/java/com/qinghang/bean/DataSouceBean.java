package com.qinghang.bean;


import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
//import org.springframework.stereotype.Component;


@Configuration
public class DataSouceBean {
    @Value("${db.serverName}")
    private String serverName;

    @Value("${db.portNumber}")
    private Integer portNumber;

    @Value("${db.databaseName}")
    private String databaseName;

    @Value("${db.user}")
    private String user;

    @Value("${db.password}")
    private String password;

    @Value("${db.rewriteBatchedInserts}")
    private Boolean rewriteBatchedInsert;

    @Value("${db.applicationName}")
    private String applicationName;
    @Bean("ds")
    public PGSimpleDataSource getDS(){
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName(serverName);
        ds.setPortNumber(portNumber);
        ds.setDatabaseName(databaseName);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setReWriteBatchedInserts(rewriteBatchedInsert); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName(applicationName);
        return ds;
    }
}
