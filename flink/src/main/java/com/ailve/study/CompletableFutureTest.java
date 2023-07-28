package com.ailve.study;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author ml.wang
 * @Date 2023-01-17
 */
public class CompletableFutureTest {

    private static String createSql = "CREATE TABLE metaspace_test (\n" +
            "`f1` STRING,\n" +
            "`f2` INTEGER,\n" +
            "`f3` TIMESTAMP(3)\n" +
            ")\n" +
            "WITH (\n" +
            "'connector' = 'datagen',\n" +
            "'rows-per-second' = '1'\n" +
            ")";

    private static String kafkaCreate = "CREATE TABLE metaspace_kafka (\n" +
            "  `f1` STRING,\n" +
            "  `f2` INTEGER,\n" +
            "  `f3` TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'trush',\n" +
            "  'properties.bootstrap.servers' = 'dataflow01.hexinfo.com:9092,dataflow02.hexinfo.com:9092,dataflow03.hexinfo.com:9092',\n" +
            "  'properties.group.id' = 'metaspace-test',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";

    private static ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactory() {
        private AtomicInteger id = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("sql-pipeline-preview-thread-" + id.addAndGet(1));
            return thread;
        }
    });

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createSql);

        tEnv.executeSql(kafkaCreate);

        List<TableResult> tableResults = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            tableResults.add(tEnv.executeSql("INSERT INTO metaspace_kafka select * from metaspace_test"));
        }

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            System.out.println("[executor service] Current Thread: " + Thread.currentThread());
            tableResults.parallelStream().forEach(tableResult -> {

                tableResult.getJobClient().ifPresent(jobClient -> {
                    try {
                        jobClient.getJobExecutionResult().get(); // 这里会一直等待直到完成或被取消
                    } catch (Exception e) { // 当job运行异常时此处会抛出异常
                        System.out.println(e.getCause());
                    }
                });
            });
        });

        try {
            future.get(20, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            //
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            //
        } finally {
            if (!future.isDone() || !future.isCancelled()) {
                boolean status = future.cancel(true);
                System.out.println("CompletableFuture Cancel status: " + status);
            }
        }

        if (!tableResults.isEmpty()) {
            tableResults.parallelStream().forEach(tableResult -> {
                tableResult.getJobClient().ifPresent(JobClient::cancel);
            });
        }


    }

}
