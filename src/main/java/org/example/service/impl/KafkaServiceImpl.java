package org.example.service.impl;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.KafkaMirrorDTO;
import org.example.dto.KafkaOffsetAdjust;
import org.example.dto.TopicData;
import org.example.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


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
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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

  public void clearKafkaLag(String payload)
  {
    try
    {
      List<KafkaMirrorDTO> kafkaMirrorDTOList = objectMapper.readValue(payload, new TypeReference<List<KafkaMirrorDTO>>(){});

      if(CollectionUtils.isNotEmpty(kafkaMirrorDTOList)) {
        for (KafkaMirrorDTO kafkaMirrorDTO : kafkaMirrorDTOList) {
          Properties props = new Properties();
          props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaMirrorDTO.getSourceBootstrapServers());
          props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaMirrorDTO.getConsumerGroupId());
          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

          // Step 2: Create Kafka consumer
          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

          // Define the specific partition you want to clear the lag from
          TopicPartition partition = new TopicPartition(kafkaMirrorDTO.getSourceTopic(), kafkaMirrorDTO.getSourcePartition());

          // Step 3: Assign the consumer to the partition
          consumer.assign(Collections.singletonList(partition));

          // Step 4: Fetch the latest offset (end offset) of the partition
          consumer.seekToEnd(Collections.singletonList(partition));
          long endOffset = consumer.position(partition);
          System.out.println("End offset for partition " + kafkaMirrorDTO.getSourcePartition() + ": " + endOffset);

          // Step 5: Commit the latest offset for this partition to clear the lag
          Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
          offsetsToCommit.put(partition, new OffsetAndMetadata(endOffset));

          // Commit the offset to mark all messages as read
          consumer.commitSync(offsetsToCommit);
          System.out.println("Lag cleared for partition " + kafkaMirrorDTO.getSourcePartition() + ". Offset committed: " + endOffset);

          // Step 6: Close the consumer
          consumer.close();
        }
      }
    }
    catch (Exception e)
    {
      log.error("Error", e);
    }
  }

  public void lagChecker(String payload)
  {
    try {
      KafkaOffsetAdjust kafkaOffsetAdjust = objectMapper.readValue(payload, KafkaOffsetAdjust.class);

      calculateLagForAllTopics(kafkaOffsetAdjust.getBootstrapServers());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public static AdminClient createAdminClient(String bootstrapServers) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return AdminClient.create(properties);
  }

  // Method to get the latest offsets for all topics
  public static Map<TopicPartition, Long> getLatestOffsets(AdminClient adminClient, Set<String> topics) throws ExecutionException, InterruptedException {
    Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();

    for (String topic : topics) {
      adminClient.describeTopics(Collections.singleton(topic)).all().get().forEach((topicName, topicDescription) -> {
        topicDescription.partitions().forEach(partition -> {
          TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
          requestLatestOffsets.put(topicPartition, OffsetSpec.latest());
        });
      });
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient.listOffsets(requestLatestOffsets).all().get();

    Map<TopicPartition, Long> latestOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : endOffsets.entrySet()) {
      latestOffsets.put(entry.getKey(), entry.getValue().offset());
    }
    return latestOffsets;
  }

  // Method to get all active consumer groups
  public static Set<String> getConsumerGroups(AdminClient adminClient) throws ExecutionException, InterruptedException {
    Set<String> consumerGroups = new HashSet<>();
    adminClient.listConsumerGroups().all().get().forEach(consumerGroupListing -> consumerGroups.add(consumerGroupListing.groupId()));
    return consumerGroups;
  }

  // Method to get consumer group offsets
  public static Map<TopicPartition, Long> getConsumerGroupOffsets(AdminClient adminClient, String consumerGroupId) throws ExecutionException, InterruptedException {
    ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
    return offsetsResult.partitionsToOffsetAndMetadata().get().entrySet().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue().offset()), HashMap::putAll);
  }

  // Method to get all topics in the Kafka cluster
  public static Set<String> getAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
    Set<String> topics = new HashSet<>();
    adminClient.listTopics().listings().get().forEach(topicListing -> topics.add(topicListing.name()));
    return topics;
  }

  // Method to calculate and print the consumer lag for all topics across all consumer groups
  public static void calculateLagForAllTopics(String bootstrapServers) throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = createAdminClient(bootstrapServers)) {

      // Fetch all topics
      Set<String> allTopics = getAllTopics(adminClient);
      System.out.println("Topics in Kafka: " + allTopics);

      // Fetch latest offsets for all topics
      Map<TopicPartition, Long> latestOffsets = getLatestOffsets(adminClient, allTopics);

      // Fetch all active consumer groups
      Set<String> consumerGroups = getConsumerGroups(adminClient);
      System.out.printf("%-40s %-30s %-15s %-15s %-15s %-15s\n", "ConsumerGroup", "Topic", "Partition", "LatestOffset", "ConsumerOffset" , "Lag");

      // For each consumer group, get the current offsets and calculate the lag
      for (String consumerGroupId : consumerGroups) {

          // Fetch current consumer group offsets
          Map<TopicPartition, Long> consumerOffsets = getConsumerGroupOffsets(adminClient, consumerGroupId);

          // Calculate lag for each partition
          for (Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            long latestOffset = entry.getValue();
            long consumerOffset = consumerOffsets.getOrDefault(partition, 0L);
            if(consumerOffset > 0)
            {
              long lag = latestOffset - consumerOffset;

              if(lag > 0)
              {
                //System.out.println("Consumer Lag for group: " + consumerGroupId);

                System.out.printf("%-40s %-30s %-15d %-15d %-15d %-15d\n", consumerGroupId, partition.topic(), partition.partition(), latestOffset, consumerOffset, lag);

//                System.out.println("Topic: " + partition.topic() +
//                        ", Partition: " + partition.partition() +
//                        ", Latest Offset: " + latestOffset +
//                        ", Consumer Offset: " + consumerOffset +
//                        ", Lag: " + lag);
              }
            }
        }
      }
    }
  }

  public void compare2BootStrap(String sourceBootstrap, String destinationBootstrap) {
    // Bootstrap servers for two Kafka clusters
    try
    {
      // Fetch partition and consumer group data for both clusters
      System.out.println("Fetching data from source cluster...");
      Map<String, TopicData> sourceClusterData = fetchKafkaClusterData(sourceBootstrap);

      System.out.println("\nFetching data from destination cluster...");
      Map<String, TopicData> destinationClusterData = fetchKafkaClusterData(destinationBootstrap);

      // Compare results between two clusters
      System.out.println("\nComparison between Source and Destination Clusters:");
      compareClusters(sourceClusterData, destinationClusterData);
    }
    catch (Exception e)
    {
      log.error("unable to compare");
    }

  }

  // Fetch the topics, partition counts, and consumer groups for a Kafka cluster
  private static Map<String, TopicData> fetchKafkaClusterData(String bootstrapServers) throws ExecutionException, InterruptedException {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient adminClient = AdminClient.create(adminProps);

    // Step 1: List all topics
    Set<String> topics = adminClient.listTopics().names().get();

    // Step 2: Fetch partition counts for each topic
    DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
    Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();

    // Step 3: Fetch all consumer groups
    ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
    Collection<ConsumerGroupListing> consumerGroupListings = consumerGroupsResult.all().get();

    // Store the topic data (partition count and consumer group count)
    Map<String, TopicData> topicDataMap = new HashMap<>();

    // Initialize topic data with partition counts
    for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
      String topicName = entry.getKey();
      int partitionCount = entry.getValue().partitions().size();
      topicDataMap.put(topicName, new TopicData(partitionCount, 0));
    }

    // Map consumer groups to topics
    for (ConsumerGroupListing consumerGroupListing : consumerGroupListings) {
      String groupId = consumerGroupListing.groupId();
      // For each consumer group, list its topics (for simplicity we assume groups subscribe to topics directly)
      // Note: In real cases, you'd need to describe the consumer group to get its topics and partitions.
      // But here we're keeping it simple with assumed 1-to-1 topic-group mapping.

      // Increment the consumer group count for each topic (simple assumption)
      for (String topic : topics) {
        TopicData topicData = topicDataMap.get(topic);
        if (topicData != null) {
          topicData.incrementConsumerGroupCount();
        }
      }
    }

    adminClient.close();
    return topicDataMap;
  }

  // Compare the source and destination cluster topic data
  private static void compareClusters(Map<String, TopicData> sourceData, Map<String, TopicData> destinationData) {
    for (String topic : sourceData.keySet()) {
      TopicData sourceTopicData = sourceData.get(topic);
      TopicData destinationTopicData = destinationData.get(topic);

      if (destinationTopicData == null) {
        System.out.println("Topic " + topic + " exists only in the source cluster.");
      } else {
        if( sourceTopicData.getPartitionCount() != destinationTopicData.getPartitionCount())
        {
          System.out.println("Topic: " + topic);
          System.out.println("  Source cluster - Partitions: " + sourceTopicData.getPartitionCount());
          System.out.println("  Destination cluster - Partitions: " + destinationTopicData.getPartitionCount());
        }
      }
    }

    // Check if there are topics in the destination cluster that don't exist in the source
    for (String topic : destinationData.keySet()) {
      if (!sourceData.containsKey(topic)) {
        System.out.println("Topic " + topic + " exists only in the destination cluster.");
      }
    }
  }

  public void getStatusOfConsumerGroup(String bootstrapServers)
  {
    // Create AdminClient to communicate with the Kafka cluster
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient adminClient = AdminClient.create(adminProps);

    try {
      //List all consumer groups
      Collection<ConsumerGroupListing> consumerGroups = adminClient.listConsumerGroups().valid().get();
      System.out.printf("%-80s %-15s %-10s %n", "Consumer Group ID", "Status", "Members");

      // Describe each consumer group to get their status
      for (ConsumerGroupListing groupListing : consumerGroups) {
        String groupId = groupListing.groupId();
        ConsumerGroupDescription description = getConsumerGroupDescription(adminClient, groupId);
        System.out.printf("%-80s %-15s %-10d %n",
                  groupId,
                  description.state(),
                  description.members().size());

          /*System.out.println("Consumer Group ID: " + groupId);
          System.out.println("  Status: " + description.state());
          System.out.println("  Members: " + description.members().size());
          //System.out.println("  Partition Assignments: " + description.partitions().size());
          System.out.println();*/
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      // Close the AdminClient
      adminClient.close();
    }
  }

  @Override
  public void postMessagesTopic() {

  }

  private static ConsumerGroupDescription getConsumerGroupDescription(AdminClient adminClient, String groupId) throws ExecutionException, InterruptedException {
    DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(Collections.singletonList(groupId));
    return describeConsumerGroupsResult.all().get().get(groupId);
  }


  public void fetchLaggedMessages()
  {

    String topic = ""; // Change this to your topic
    String bootstrapServers = ""; // Change this to your bootstrap servers
    String groupId = ""; // Change this to your consumer group ID

    // Create Kafka Consumer properties
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit

    // Create Kafka Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    // Get partition information for the topic
    List<TopicPartition> partitions = new ArrayList<>();
    for (PartitionInfo partition : consumer.partitionsFor(topic)) {
      partitions.add(new TopicPartition(partition.topic(), partition.partition()));
    }

    // Create AdminClient for fetching latest offsets
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient adminClient = AdminClient.create(adminProps);

    try {
      // Get the latest offsets for each partition
      Map<TopicPartition, Long> latestOffsets = getLatestOffsets(adminClient, partitions);

      // Get committed offsets for the consumer group
      Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(new HashSet<>(partitions));

      // Print lag and fetch lagging messages for each partition
      for (TopicPartition partition : partitions) {
        long latestOffset = latestOffsets.get(partition);
        OffsetAndMetadata committedOffsetAndMetadata = committedOffsets.get(partition);
        long committedOffset = committedOffsetAndMetadata != null ? committedOffsetAndMetadata.offset() : OffsetFetchResponse.INVALID_OFFSET;

        if (committedOffset == OffsetFetchResponse.INVALID_OFFSET) {
          System.out.printf("Partition: %d has no committed offset. Fetching from offset 0.%n", partition.partition());
          fetchMessagesFromOffset(consumer, partition, 0, latestOffset);
        } else {
          long lag = latestOffset - committedOffset;
          System.out.printf("Partition: %d Lag: %d messages%n", partition.partition(), lag);
          if (lag > 0) {
            fetchMessagesFromOffset(consumer, partition, committedOffset, latestOffset);
          }
        }
      }

    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      adminClient.close();
      consumer.close();
    }
  }

  private static Map<TopicPartition, Long> getLatestOffsets(AdminClient adminClient, List<TopicPartition> partitions) throws ExecutionException, InterruptedException {
    Map<TopicPartition, Long> latestOffsets = new HashMap<>();

    for (TopicPartition partition : partitions) {
      Map<TopicPartition, OffsetSpec> request = Collections.singletonMap(partition, OffsetSpec.latest());
      ListOffsetsResult result = adminClient.listOffsets(request);
      ListOffsetsResult.ListOffsetsResultInfo resultInfo = result.partitionResult(partition).get();
      latestOffsets.put(partition, resultInfo.offset());
    }
    return latestOffsets;
  }

  // Fetch and print messages from a specific offset to the latest offset
  private static void fetchMessagesFromOffset(KafkaConsumer<String, String> consumer, TopicPartition partition, long startOffset, long endOffset) {
    // Assign partition and seek to the starting offset
    consumer.assign(Collections.singletonList(partition));
    consumer.seek(partition, startOffset);

    System.out.printf("Fetching messages from partition %d from offset %d to %d%n", partition.partition(), startOffset, endOffset);

    // Fetch and print messages
    boolean continueFetching = true;
    while (continueFetching) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                record.partition(), record.offset(), record.key(), record.value());
        if (record.offset() >= endOffset - 1) {
          continueFetching = false;
          break;
        }
      }
    }
  }



    /*// Kafka AdminClient properties
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    // Create AdminClient instance
    try (AdminClient adminClient = AdminClient.create(properties)) {
      // Get consumer group description
      ConsumerGroupDescription groupDescription = adminClient.describeConsumerGroups(
                      java.util.Collections.singletonList(groupId))
              .describedGroups()
              .get(groupId)
              .get();

      // Get the list of members in the consumer group
      Collection<MemberDescription> members = groupDescription.members();

      if (members.isEmpty()) {
        System.out.println("No members in consumer group: " + groupId);
      } else {
        // Print header for the table
        System.out.printf("%-30s%-20s%-30s%n", "ConsumerMemberID", "Host", "AssignedPartitions");
        System.out.println("--------------------------------------------------------------------------------------------");

        // For each member, print the partitions assigned to them
        for (MemberDescription member : members) {
          // Get partitions assigned to the member for the specific topic
          StringJoiner joiner = new StringJoiner(", ");
          for (TopicPartition tp : member.assignment().topicPartitions()) {
            if (tp.topic().equals(topic)) {
              String s = String.valueOf(tp.partition());
              joiner.add(s);
            }
          }
          String partitions = joiner.toString();

          // Print in table format
          System.out.printf("%-30s%-20s%-30s%n", member.consumerId(), member.host(), partitions.isEmpty() ? "No partitions" : partitions);
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }*/

    /*// Kafka Bootstrap server address (your Kafka broker address)
    String bootstrapServers = ""; // Replace with your Kafka broker IP:port
    String groupId = "";       // Your consumer group ID
    String topic = "";                  // The topic you want to consume from

    // Properties configuration for the Kafka Consumer
    Properties properties = new Properties();

    // Pointing to the Kafka broker
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    // Assign a consumer group ID, consumers in the same group share the load
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    // Configure key and value deserializers (e.g., String key-value pairs)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // Define the starting offset behavior if no committed offset is found for the consumer
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // or "latest" for most recent

    // Enable auto-commit of offsets (you can turn this off and handle offsets manually if needed)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

    // Create a Kafka Consumer instance
    Consumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Subscribe to the specified topic
    consumer.subscribe(Collections.singletonList(topic));

    // Poll for new records in a loop (simple infinite loop)
    try {
      for(int i = 0; i< 20; i++)
      {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        if(Objects.nonNull(records))
        {
          for (ConsumerRecord<String, String> record : records) {
            System.out.printf("Consumed record: key = %s, value = %s, partition = %d, offset = %d%n",
                    record.key(), record.value(), record.partition(), record.offset());
          }
        }
        // Process each record from the topic

      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close(); // Close the consumer on exit
    }*/

//    List<String> bootstrapServers = Arrays.asList(""); // example bootstrap servers
//
//    for (String server : bootstrapServers) {
//      String jmxUrl = "service:jmx:rmi:///jndi/rmi://" + server + "/jmxrmi";
//      try {
//        JMXServiceURL serviceUrl = new JMXServiceURL(jmxUrl);
//        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
//        MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
//
//        // Example: Get Under-replicated Partitions
//        ObjectName underReplicatedPartitions = new ObjectName("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions");
//        Integer underReplicatedCount = (Integer) mBeanServerConnection.getAttribute(underReplicatedPartitions, "Value");
//        System.out.println("Under-replicated partitions for " + server + ": " + underReplicatedCount);
//
//        // You can retrieve more metrics as needed
//
//        // Close JMX connection
//        jmxConnector.close();
//      } catch (Exception e) {
//        System.err.println("Failed to connect to " + server + ": " + e.getMessage());
//      }
//    }


    /*// Define source and target Kafka clusters (bootstrap servers)
    String sourceBootstrapServers = "";  // Replace with source Kafka broker
    String targetBootstrapServers = "";  // Replace with target Kafka broker
    String sourceTopic = "";                          // Replace with source Kafka topic
    String targetTopic = "";                          // Replace with target Kafka topic
    String consumerGroupId = "";      // Replace with a unique consumer group ID

    // Maximum number of messages to relay
    int maxMessages = 5000;
    int messageCount = 0;

    // Create Kafka consumer properties for the source cluster
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrapServers);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // Start from the beginning if no offset is committed

    // Create Kafka producer properties for the target cluster
    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetBootstrapServers);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Kafka consumer and producer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
    KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

    // Subscribe the consumer to the source topic
    consumer.subscribe(Collections.singletonList(sourceTopic));

    try {
      // Poll and relay messages from source to target
      while (messageCount < maxMessages) {
        // Poll for new messages from the source cluster
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        // For each record consumed from the source topic, send it to the target topic
        for (ConsumerRecord<String, String> record : records) {
          // Prepare a record to send to the target cluster (topic, key, value)
          ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, record.key(), record.value());

          // Send the record to the target cluster
          producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
              System.err.println("Error sending message to target cluster: " + exception.getMessage());
            } else {
              System.out.println("Message sent to target topic " + metadata.topic() +
                      " partition " + metadata.partition() + " offset " + metadata.offset());
            }
          });

          // Increment the message count
          messageCount++;

          // Break the loop if we've sent the max number of messages
          if (messageCount >= maxMessages) {
            System.out.println("Reached the limit of " + maxMessages + " messages. Exiting.");
            break;
          }
        }
        // Flush to ensure all messages are sent
        producer.flush();
      }
    } finally {
      // Close both the consumer and producer to release resources
      consumer.close();
      producer.close();
    }*/

}
