package com.johnnyzheng.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ConsumerMutiThreadTest {

    private static Logger LOG = LoggerFactory.getLogger(ConsumerRebalanceListener.class);
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.137.129:9092,192.168.137.130:9092,192.168.137.131:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zzn-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //一次拉取条数
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);
        //设置服务器发送到客户端的最小数据,1表示立即接收
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,2);
        //如果在此时间内还没有满足FETCH_MIN_BYTES_CONFIG，则返回
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,5000);

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        for (int i=0;i<3;i++){
            KafkaConsumer<String, String> kafkaConsumer;
            kafkaConsumer = new KafkaConsumer<>(props);
            ConsumeThread consumeThread = new ConsumeThread(kafkaConsumer,"HelloKafkaTopic");
            executorService.execute(consumeThread);
        }

    }



}
