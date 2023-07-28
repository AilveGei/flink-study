package com.ailve.study;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 将 where 中的过滤条件下推到 Source 中进行处理 验证
 * @Author ml.wang
 * @Date 2023-07-28
 */
public class FilterPushDown {

    private static String createSql = "CREATE TABLE orders_mysql (\n" +
            "\t`order_id` int,\n" +
            "\t`order_date` timestamp(3),\n" +
            "\t`customer_id` bigint,\n" +
            "\t`price` decimal(10,5),\n" +
            "\t`product_id` int\n" +
            ") WITH (\n" +
            "\t'connector' = 'jdbc',\n" +
            "\t'url' = 'jdbc:mysql://node2.hexinfo.com:3306/demo?serverTimezone=Asia/Shanghai&characterEncoding=utf8&useSSL=false',\n" +
            "\t'username' = 'root',\n" +
            "\t'password' = 'Shhex!1324',\n" +
            "\t'table-name' = 'orders'\n" +
            ")";

    private static String createPrintSql =  "CREATE TABLE orders_print (\n" +
            "\t`order_id` int,\n" +
            "\t`order_date` string,\n" +
            "\t`customer_id` bigint,\n" +
            "\t`price` decimal(10,5),\n" +
            "\t`product_id` int\n" +
            ") WITH (\n" +
            "\t'connector' = 'print'\n" +
            ")";

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8888);
        // 创建 开启 webui 的流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createSql);

        tEnv.executeSql(createPrintSql);

        tEnv.executeSql(" insert into orders_print " +
                "select " +
                "order_id, " +
                "DATE_FORMAT(order_date, 'yyyy-MM-dd HH:mm:dd') as order_date, " +
                "customer_id, " +
                "price, " +
                "product_id  " +
                "from orders_mysql " +
                "where order_id = 10001 AND customer_id = 100001 LIMIT 1");

    }

}
