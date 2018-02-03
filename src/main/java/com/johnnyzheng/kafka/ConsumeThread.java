package com.johnnyzheng.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by huadiefei on 2018/1/30.
 */
public class ConsumeThread extends  Thread {

    private KafkaConsumer<String, String> kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private String topic;

    public  ConsumeThread(KafkaConsumer<String, String> kafkaConsumer,String topic){
        this.kafkaConsumer = kafkaConsumer;
        this.topic = topic;
    }

    @Override
    public void run() {
        //订阅Topic
        kafkaConsumer.subscribe(Arrays.asList(topic), new HandleRebalance());

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
                            System.out.println("Commit failed for offsets:"+offsets);
                            System.out.println("error info:"+exception);
                        }
//                    System.out.println("提交偏移成功："+ offsets);
                    }
                });
            }

        }

    }


    private class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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
}



