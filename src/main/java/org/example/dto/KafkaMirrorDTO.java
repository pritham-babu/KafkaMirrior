package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class KafkaMirrorDTO implements Serializable {

    private String sourceBootstrapServers;
    private String destinationBootstrapServers;
    private String sourceTopic;
    private String destinationTopic;
    private String consumerGroupId;
    private int sourcePartition;
    private long sourceOffset;
    private int destinationPartition;
    private long resetOffset;
    private List<String> finaltest;
}
