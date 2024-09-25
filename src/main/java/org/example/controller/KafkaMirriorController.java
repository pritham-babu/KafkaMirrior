package org.example.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.example.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("/v1.0")
@Slf4j
@CrossOrigin
@Api(value = "Kafka utils")
public class KafkaMirriorController {

    @Autowired
    KafkaService kafkaService;
    @PostMapping(value = "kafkaMessageCopierWithOffset",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Kafka Message Copier with Offset", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void kafkaMessageCopierWithOffset(@RequestBody String payload)
    {
        kafkaService.kafkaMessageCopierWithOffset(payload);
    }

    @PostMapping(value = "kafkaOffsetsAndPartitionFetcher",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Kafka Offsets And Partition Fetcher", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public ResponseEntity<?> kafkaOffsetsAndPartitionFetcher(@RequestBody String payload)
    {
        return new ResponseEntity<>(kafkaService.kafkaOffsetsAndPartitionFetcher(payload), OK);
    }
}
