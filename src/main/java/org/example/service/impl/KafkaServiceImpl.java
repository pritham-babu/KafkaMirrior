package org.example.service.impl;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.KafkaMirrorDTO;
import org.example.dto.KafkaOffsetAdjust;
import org.example.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Service
@Slf4j
//@Conditional(value = QueryDatabaseCondition.class)
public class KafkaServiceImpl implements KafkaService {


  @Autowired
  ObjectMapper objectMapper;

  public void kafkaMessageCopierWithOffset(String payload)
  {
    try
    {
      List<KafkaMirrorDTO> kafkaMirrorDTOList = objectMapper.readValue(payload, new TypeReference<List<KafkaMirrorDTO>>(){});

      if(CollectionUtils.isNotEmpty(kafkaMirrorDTOList))
      {
        for(KafkaMirrorDTO kafkaMirrorDTO: kafkaMirrorDTOList)
        {
          String sourceBootstrapServers = kafkaMirrorDTO.getSourceBootstrapServers();
          String destinationBootstrapServers = kafkaMirrorDTO.getDestinationBootstrapServers();
          String sourceTopic = kafkaMirrorDTO.getSourceTopic();
          String destinationTopic = kafkaMirrorDTO.getDestinationTopic();
          String consumerGroupId = kafkaMirrorDTO.getConsumerGroupId();


          Map<Integer, Long> partitionOffsets = new HashMap<>();
          partitionOffsets.put(kafkaMirrorDTO.getSourcePartition(), kafkaMirrorDTO.getSourceOffset());

          // Consumer configuration for the source Kafka cluster
          Properties consumerProperties = new Properties();
          consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrapServers);
          consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
          consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

          // Producer configuration for the destination Kafka cluster
          Properties producerProperties = new Properties();
          producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBootstrapServers);
          producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
          producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

          createConsumerGroupInDestinationServerIfnotPresent(consumerProperties, consumerGroupId, kafkaMirrorDTO);

          // Create Kafka Consumer for the source cluster
          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

          // Create Kafka Producer for the destination cluster
          KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

          // Assign specific partitions to the consumer and seek to the starting offsets
          List<TopicPartition> partitions = new ArrayList<>();
          Map<TopicPartition, Long> endOffsets = new HashMap<>();
          for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
            TopicPartition partition = new TopicPartition(sourceTopic, entry.getKey());
            partitions.add(partition);
            consumer.assign(Collections.singletonList(partition));
            consumer.seek(partition, entry.getValue());
          }

          // End offsets to know when to stop consuming for each partition
          endOffsets.putAll(consumer.endOffsets(partitions));

          try {
            boolean done = false;
            while (!done) {
              // Poll messages from the source topic
              ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

              for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                        record.key(), record.value(), record.partition(), record.offset());

                // Create a ProducerRecord to send to the destination topic for specified partition
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(destinationTopic,
                        kafkaMirrorDTO.getDestinationPartition(), record.key(), record.value());

                // Send the record to the destination cluster
                producer.send(producerRecord);
              }

              // Manually commit the offsets of the consumed messages
              consumer.commitSync();

              done = true;

              for (TopicPartition partition : partitions) {

                Map<TopicPartition, Long> endOffsets1 = consumer.endOffsets(Collections.singletonList(partition));
                long endOffset = endOffsets1.get(partition);
                System.out.println("End Offset: " + endOffset);

                // Get the committed offset for the consumer group on this partition
                OffsetAndMetadata committed = consumer.committed(partition);
                long committedOffset = (committed != null) ? committed.offset() : 0;
                System.out.println("Committed Offset: " + committedOffset);

                // Calculate the lag
                long lag = endOffset - committedOffset;
                System.out.println("Current Lag: " + lag);
                if (lag > 0) {
                  done = false;  // More messages to process in this partition
                  break;
                }
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            // Close the producer and consumer
            producer.close();
            consumer.close();
          }
        }
      }
    }
    catch (Exception e)
    {
      log.error("Unable to copy message from one cluster to another cluster", e);
    }

  }

  private void createConsumerGroupInDestinationServerIfnotPresent(Properties producerProperties, String consumerGroupId, KafkaMirrorDTO kafkaMirrorDTO) {

    try
    {
      AdminClient adminClient = AdminClient.create(producerProperties);
      boolean groupExists = doesConsumerGroupExist(adminClient, consumerGroupId);
      if (!groupExists) {
        System.out.println("Consumer group does not exist. Creating a new consumer group...");

        // Step 2: Create a consumer group by starting a consumer
        createConsumerGroup(kafkaMirrorDTO.getDestinationBootstrapServers(), consumerGroupId,
                kafkaMirrorDTO.getDestinationTopic(), kafkaMirrorDTO.getDestinationPartition());
      }
    }
    catch (Exception e)
    {
      log.error("Unable to check the group id ", e);
    }
  }

  public List<Map> kafkaOffsetsAndPartitionFetcher(String payload) {
    List<Map> topicDetails = new ArrayList<>();

    try {
      KafkaOffsetAdjust kafkaOffsetAdjust = objectMapper.readValue(payload, KafkaOffsetAdjust.class);

      // Kafka bootstrap servers
      String bootstrapServers = kafkaOffsetAdjust.getBootstrapServers();

      // AdminClient properties to interact with Kafka
      Properties adminProps = new Properties();
      adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

      // Kafka AdminClient to fetch consumer groups
      try (AdminClient adminClient = AdminClient.create(adminProps)) {
        // Get all consumer groups
        List<ConsumerGroupListing> consumerGroupListings = new ArrayList<>(adminClient.listConsumerGroups().all().get());

        for (ConsumerGroupListing consumerGroupListing : consumerGroupListings) {
          String consumerGroupId = consumerGroupListing.groupId();

          // Fetch the committed offsets for this consumer group
          ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
          Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();

          // Construct map with source, destination cluster ip, topic names, group id, offset and partitions
          for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Map topicValues = new HashMap();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            topicValues.put("sourceBootstrapServers", bootstrapServers);
            topicValues.put("destinationBootstrapServers", kafkaOffsetAdjust.getDestinationBootstrapServers());
            topicValues.put("sourceTopic", topicPartition.topic());
            topicValues.put("destinationTopic", topicPartition.topic());
            topicValues.put("consumerGroupId", consumerGroupId);
            topicValues.put("sourcePartition", topicPartition.partition());
            topicValues.put("destinationPartition",  topicPartition.partition());
            topicValues.put("sourceOffset", offsetAndMetadata.offset());
            topicDetails.add(topicValues);
          }
        }
      } catch (ExecutionException | InterruptedException e) {
        log.error("Unable to fetch details", e);
      }
    } catch (Exception e) {
      log.error("Unable to fetch details", e);
    }
    return topicDetails;
  }

  public void kafkaResetOffset(String payload)
  {
    try
    {
      List<KafkaMirrorDTO> kafkaMirrorDTOList = objectMapper.readValue(payload, new TypeReference<List<KafkaMirrorDTO>>(){});

      if(CollectionUtils.isNotEmpty(kafkaMirrorDTOList)) {
        for (KafkaMirrorDTO kafkaMirrorDTO : kafkaMirrorDTOList) {
          Properties props = new Properties();
          props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMirrorDTO.getSourceBootstrapServers());
          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaMirrorDTO.getConsumerGroupId());
          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
          try
          {
            TopicPartition topicPartition = new TopicPartition(kafkaMirrorDTO.getSourceTopic(), kafkaMirrorDTO.getSourcePartition());
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.seek(topicPartition, kafkaMirrorDTO.getResetOffset());
          }
          catch (Exception e)
          {
            log.error("Reset error", e);
          }
          finally {
            consumer.close();
          }
        }
      }
    }
    catch (Exception e)
    {
      log.error("Unable to reset offset", e);
    }
  }


  private static boolean doesConsumerGroupExist(AdminClient adminClient, String groupId) throws ExecutionException, InterruptedException {
    try {
      boolean isPresent = false;
      KafkaFuture<ConsumerGroupDescription> future = adminClient.describeConsumerGroups(Collections.singletonList(groupId))
              .describedGroups().get(groupId);
      // If the group exists, no exception will be thrown, and we return true
      ConsumerGroupDescription groupDescription = future.get();
      if(groupDescription!=null && ConsumerGroupState.DEAD.equals(groupDescription.state()))
      {
        isPresent = true;
      }
      return isPresent;
    } catch (Exception e) {
      // If the group doesn't exist, an exception may be thrown
      return false;
    }
  }

  // Method to create a consumer group by starting a consumer
  private static void createConsumerGroup(String bootstrapServers, String groupId, String topic, int partition) {
    // Consumer configuration properties
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // Start consuming from the earliest available offset

    // Create Kafka consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    // Assign the consumer to a specific topic partition
    consumer.assign(Collections.singletonList(topicPartition));

    // Poll the topic to initiate the consumer group creation
    consumer.poll(Duration.ofMillis(1000));
    System.out.println("Consumer group created and joined topic: " + topic);

    // Close the consumer after the group has been created
    consumer.close();
  }
}
