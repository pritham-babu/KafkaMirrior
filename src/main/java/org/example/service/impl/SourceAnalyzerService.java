package org.example.service.impl;

import org.example.service.AnalysisResult;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

@Service
public class SourceAnalyzerService {
    private static final Map<String, String> riskyPatterns = new HashMap<>();
    static {
        riskyPatterns.put(".forName\\(", "Use dependency injection instead of dynamic loading.");
        riskyPatterns.put(".invoke\\(", "Avoid reflection, prefer interface-based dispatch.");
        riskyPatterns.put("field\\.set\\(", "Use regular field access when possible.");
        riskyPatterns.put("field\\.get\\(", "Use getters/setters instead.");
        riskyPatterns.put("Field\\.set\\(", "Use regular field access when possible.");
        riskyPatterns.put("Field\\.get\\(", "Use getters/setters instead.");
        riskyPatterns.put("Proxy\\.newProxyInstance\\(", "Avoid runtime proxies; prefer compile-time alternatives.");
        riskyPatterns.put("ClassLoader", "Custom ClassLoaders can cause leaks if not managed well.");
        riskyPatterns.put("cglib", "Framework proxy — optimize bean scopes or avoid deep proxy chains.");
        riskyPatterns.put("bytebuddy", "Review for unnecessary dynamic proxy creation.");
    }

    public List<AnalysisResult> analyzeSource(String sourcePath) {
        List<AnalysisResult> results = new ArrayList<>();
        Path basePath = Paths.get(sourcePath);

        try {
            Files.walk(basePath)
                    .filter(path -> path.toString().endsWith(".java"))
                    .forEach(file -> results.addAll(analyzeFile(file)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;
    }

    private List<AnalysisResult> analyzeFile(Path file) {
        List<AnalysisResult> results = new ArrayList<>();
        try {
            List<String> lines = Files.readAllLines(file);
            int lineNumber = 0;
            for (String line : lines) {
                lineNumber++;
                for (Map.Entry<String, String> entry : riskyPatterns.entrySet()) {
                    Pattern pattern = Pattern.compile(entry.getKey());
                    if (pattern.matcher(line).find()) {
                        results.add(new AnalysisResult(
                                file.toString(),
                                lineNumber,
                                line.trim(),
                                entry.getKey(),
                                entry.getValue()
                        ));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }
}
