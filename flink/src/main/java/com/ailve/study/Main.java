package com.ailve.study;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Optional;

/**
 * @Author ml.wang
 * @Date ${YEAR}-${MONTH}-${DAY}
 */
public class Main {

    private static String createSql = "CREATE TABLE metaspace_test (\n" +
            "`f1` STRING,\n" +
            "`f2` INTEGER,\n" +
            "`f3` TIMESTAMP(3)\n" +
            ")\n" +
            "WITH (\n" +
            "'connector' = 'datagen',\n" +
            "'rows-per-second' = '10'\n" +
            ")";

    private static String kafkaCreate = "";

    public static void main(String[] args) {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        tEnv.executeSql(createSql);
//
//        TableResult result = tEnv.executeSql("select * from metaspace_test");
//
//        result.getJobClient().get().cancel();


        String value = Optional.ofNullable("1234").orElse("abc");
        System.out.println(value);

        String result = null;
        Optional<String> value2 = Optional.ofNullable(result);
        value2.isPresent();
        System.out.println(value2);

    }
}