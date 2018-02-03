package com.johnnyzheng.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class ConsumerRebalanceTest {

    private static Logger LOG = LoggerFactory.getLogger(ConsumerRebalanceListener.class);

    private static KafkaConsumer<String, String> kafkaConsumer;

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private static class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Lost patitions in rebalance." +
                    "Committing current offsets:" + currentOffsets);
            System.out.println("Lost patitions in rebalance. \n" +
                    "Current Patition = "+partitions+
                    "Committing current offsets:" + currentOffsets);
            //同步提交
            if (!partitions.isEmpty()){
                kafkaConsumer.commitSync(currentOffsets);
                System.out.println("Rebalance 后,同步提交成功："+ currentOffsets);
                System.out.println("Rebalance 后,同步提交成功，清空缓存");
                currentOffsets.clear();
            }

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            //nothing to do
        }
    }

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

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("HelloKafkaTopic"), new HandleRebalance());
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
            if (!records.isEmpty()){
                System.out.println("拉取到一次数据...");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date())+"Partition: " + record.partition() + " ,Offset: " + record.offset()
                            + " ,Value: " + record.value() + " ,ThreadID: " + Thread.currentThread().getId());

//                System.out.println("topic = " + record.topic() + " patition =" + record.partition() + "offset =" + record.offset());
//                LOG.info("topic = %s ,patition = %s, offsets = %d",record.topic(),record.partition(),record.offset());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "no metadata"));
            }
                //异步提交
                kafkaConsumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception!=null){
                            LOG.error("Commit failed for offsets{}",offsets,exception);
                            System.out.println("Commit failed for offsets:"+offsets);
                            System.out.println("error info:"+exception);
                        }
//                    System.out.println("提交偏移成功："+ offsets);
                    }
                });
            }

        }
    }
}
