package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class KafkaOffsetAdjust implements Serializable {

    private String bootstrapServers;
    private String topic;
    private int partition;
    private long newOffset;
    private String consumerGroupId;
    private String destinationBootstrapServers;
}
