package org.example.service;


import java.util.List;
import java.util.Map;

public interface KafkaService {
  void kafkaMessageCopierWithOffset(String payload);

  void kafkaResetOffset(String payload);

  List<Map> kafkaOffsetsAndPartitionFetcher(String payload);

  void clearKafkaLag(String payload);

  void lagChecker(String payload);

  void compare2BootStrap(String sourceBootstrap, String destinationBootstrap);

  void getStatusOfConsumerGroup(String sourceBootstrap);

}
