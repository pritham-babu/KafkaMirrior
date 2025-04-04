package org.example.controller;

import org.example.service.impl.RedisReportService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RedisController {

    private final RedisReportService reportService;

    public RedisController(RedisReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/redis/report")
    public String getRedisReport() {
        return reportService.generateReport();
    }
}