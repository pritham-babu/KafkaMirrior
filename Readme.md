Getting Started
Prerequisites
Ensure you have the following installed:

Java Development Kit (JDK)
Gradle
Steps to Run the Project Locally
Clone the Repository

bash
Copy code
git clone <repository-url>
Build the Project Navigate to the project directory and run:

bash
Copy code
./gradlew clean build
Run the Spring Boot Application Start the application with:

bash
Copy code
./gradlew bootRun
Access Swagger UI Open your browser and navigate to:

bash
Copy code
http://localhost:8080/swagger-ui.html
Kafka Mirror APIs
The application provides two APIs for Kafka mirroring:

1. Fetch Kafka Topics and Partitions
   To get the list of topics and partitions needed to copy messages, use the following API:

Endpoint:

bash
Copy code
POST http://localhost:8080/v1.0/kafkaOffsetsAndPartitionFetcher
Request Body:

json
Copy code
{
"bootstrapServers": "",
"destinationBootstrapServers": ""
}
This API will return a list of topics, groups, partitions, and offset details. Review the output to identify the topics you wish to copy.

2. Copy Kafka Messages with Offsets
   Once you have the required information, use the following API to copy messages:

Endpoint:

bash
Copy code
POST http://localhost:8080/v1.0/kafkaMessageCopierWithOffset
Sample Request Body:

json
Copy code
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
Important Notes
If the destination partition is specified, the copy operation will proceed. If it is not present, the operation will fail.
This API will copy all messages starting from the last offset for the specified partition and will also commit the offsets in the old cluster.