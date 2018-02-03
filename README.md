# 1.Kafka 测试工程
## 1.1Kafka版本
采用0.11.0.2
## 2.主要功能
1.Kafka Consumer 偏移的提交，手动提交偏移，使用同步提交和异步提交相结合的方式，可靠的提交偏移
<p>在订阅主题（Topic）时，添加HandleRebalance的监听，有新的消费者加入触发rebalance时，同步提交偏移</p>

2.尝试多线程中使用Kafka Consumer
