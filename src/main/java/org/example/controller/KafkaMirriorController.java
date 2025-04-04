package org.example.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.example.service.AnalysisResult;
import org.example.service.KafkaService;
import org.example.service.impl.SourceAnalyzerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    @Autowired
    SourceAnalyzerService analyzerService;
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



    @GetMapping
    public List<AnalysisResult> analyze(@RequestParam String sourcePath) {
        return analyzerService.analyzeSource(sourcePath);
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

    @PostMapping(value = "kafkaResetOffset",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Kafka Reset Offsets", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void kafkaResetOffset(@RequestBody String payload)
    {
        kafkaService.kafkaResetOffset(payload);
    }

    @PostMapping(value = "clearKafkaLag",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "clearKafkaLag", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void clearKafkaLag(@RequestBody String payload)
    {
        kafkaService.clearKafkaLag(payload);
    }


    @PostMapping(value = "lagChecker",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "lagChecker", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void lagChecker(@RequestBody String payload)
    {
        kafkaService.lagChecker(payload);
    }

    @PostMapping(value = "compare2BootStrap",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "compare2BootStrap", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void compare2BootStrap(@RequestParam String sourceBootstrap, @RequestParam String destinationBootstrap)
    {
        kafkaService.compare2BootStrap(sourceBootstrap, destinationBootstrap);
    }

    @PostMapping(value = "getStatusOfConsumerGroup",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "getStatusOfConsumerGroup", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void getStatusOfConsumerGroup(@RequestParam String sourceBootstrap)
    {
        kafkaService.getStatusOfConsumerGroup(sourceBootstrap);
    }

    @PostMapping(value = "postMessagesTopic",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "postMessagesTopic", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void postMessagesTopic() throws Exception
    {
        kafkaService.test();
    }

    @PostMapping(value = "kafkaManualOffsetCommit",
            produces = APPLICATION_JSON_VALUE)
    @ApiOperation(value = "kafkaManualOffsetCommit", produces = "application/json",
            consumes = "application/json")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "The Post call is Successful"),
            @ApiResponse(code = 500, message = "The Post call Failed"),
            @ApiResponse(code = 404, message = "The API could not be found"),
            @ApiResponse(code = 400, message = "Invalid input")})
    public void kafkaManualOffsetCommit(@RequestParam String message)
    {
        kafkaService.kafkaManualOffsetCommit(message);
    }
}
