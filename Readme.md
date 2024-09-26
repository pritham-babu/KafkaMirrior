# Overview
This project provides a Spring Boot application with APIs for mirroring Kafka topics. You can easily copy messages from one Kafka cluster to another by following the steps below.

# Getting Started

## Prerequisites

Before you begin, ensure you have the following installed on your machine:

- Java Development Kit (JDK)
- Gradle

## Steps to Run the Project Locally

- Clone the Repository Clone the repository to your local machine using:

~~~
git clone <repository-url>
~~~

- Build the Project Navigate to the project directory and run the following command to build the code:

~~~
./gradlew clean build
~~~

- Run the Spring Boot Application Start the application using:

~~~
./gradlew bootRun
~~~

- Access Swagger UI Open your web browser and navigate to:

~~~ 
http://localhost:8080/swagger-ui.html
~~~



# Kafka Mirror APIs

This application exposes two APIs for mirroring Kafka topics:

- ## Fetch Kafka Topics and Partitions
To retrieve a list of topics and partitions for copying messages, use the following API:

~~~
Endpoint:
POST http://localhost:8080/v1.0/kafkaOffsetsAndPartitionFetcher

Request Body:
{
  "bootstrapServers": "",
  "destinationBootstrapServers": ""
}
~~~

This request will return a list of topics, consumer groups, partitions, and offset details. Review the output to identify the topics you wish to mirror.

# Dont post same messages by multiple people. 

- ## Copy Kafka Messages with Offsets

Once you have the necessary information, you can use the following API to copy messages:

~~~
Endpoint:
POST http://localhost:8080/v1.0/kafkaMessageCopierWithOffset

Request Body:
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
~~~


### Important Notes

- Copy Operation: The copy operation will proceed only if the destination partition is specified. If it is absent, the operation will fail.
- Message Handling: This API will copy all messages starting from the last offset for the specified partition and will also commit the offsets in the old cluster.

