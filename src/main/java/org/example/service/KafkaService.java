package org.example.service;


import java.util.List;
import java.util.Map;

public interface KafkaService {
  void kafkaMessageCopierWithOffset(String payload);

  void kafkaResetOffset(String payload);

  List<Map> kafkaOffsetsAndPartitionFetcher(String payload);
}
