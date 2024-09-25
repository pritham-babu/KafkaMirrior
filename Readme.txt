1. Clone the repo to local
2. build the code locally "./gradlew clean build"
3. run the spring boot locally
4. access the swagger http://localhost:8080/swagger-ui.html
5. There are 2 apis for kafka mirrior
    a. First you need to get the list of topics and partition to copy the messages.
        Use api http://localhost:8080/v1.0/kafkaOffsetsAndPartitionFetcher with input body as
        {
          "bootstrapServers": "",
          "destinationBootstrapServers": ""
        }

    b. this will give you list of topics , group, partition and offset details. review for your topics and
    use the same to next api for kafka mirrior

    http://localhost:8080/v1.0/kafkaMessageCopierWithOffset

    sample body :

    [
      {
        "destinationBootstrapServers": "",
        "consumerGroupId": "BG_RTT_INTERNAL_UAT-BG_VIS_UAT",
        "sourcePartition": 0,
        "sourceTopic": "BG_EVENT_UAT",
        "sourceOffset": 35264317,
        "destinationPartition": 0,
        "destinationTopic": "BG_EVENT_UAT",
        "sourceBootstrapServers": ""
      },
      {
        "destinationBootstrapServers": "",
        "consumerGroupId": "BG_RTT_INTERNAL_UAT-BG_VIS_UAT",
        "sourcePartition": 2,
        "sourceTopic": "BG_EVENT_UAT",
        "sourceOffset": 35960956,
        "destinationPartition": 2,
        "destinationTopic": "BG_EVENT_UAT",
        "sourceBootstrapServers": ""
      }
    ]
6. Note : if destination partition is present then it will copy, else it will fail.
7. This api will take all messages from the last offset for that partition. and it will commit in old cluster too.