package org.example.dto;

public class TopicData {
    private int partitionCount;
    private int consumerGroupCount;

    public TopicData(int partitionCount, int consumerGroupCount) {
        this.partitionCount = partitionCount;
        this.consumerGroupCount = consumerGroupCount;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getConsumerGroupCount() {
        return consumerGroupCount;
    }

    public void incrementConsumerGroupCount() {
        this.consumerGroupCount++;
    }
}
