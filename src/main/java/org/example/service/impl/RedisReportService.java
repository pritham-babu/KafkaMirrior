package org.example.service.impl;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RedisReportService {

    private final RedisService redisService;

    public RedisReportService(RedisService redisService) {
        this.redisService = redisService;
    }

    public String generateReport() {
        List<RedisService.RedisKeyDetails> details = redisService.getAllKeyDetails();
        StringBuilder report = new StringBuilder();
        report.append("Redis Keys Report\n");
        report.append("=================\n");
        report.append(String.format("%-20s %-10s %-50s\n", "Key", "Size", "Value"));
        report.append(String.format("%-20s %-10s %-50s\n", "---", "----", "-----"));

        for (RedisService.RedisKeyDetails detail : details) {
            report.append(String.format("%-20s %-10d %-50s\n",
                    detail.getKey(), detail.getSize(), detail.getValue()));
        }

        return report.toString();
    }
}
