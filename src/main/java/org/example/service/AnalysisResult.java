package org.example.service;

public class AnalysisResult {

    private String file;
    private int line;
    private String code;
    private String issue;
    private String suggestion;

    // Constructor
    public AnalysisResult(String file, int line, String code, String issue, String suggestion) {
        this.file = file;
        this.line = line;
        this.code = code;
        this.issue = issue;
        this.suggestion = suggestion;
    }

    // Getters
    public String getFile() {
        return file;
    }

    public int getLine() {
        return line;
    }

    public String getCode() {
        return code;
    }

    public String getIssue() {
        return issue;
    }

    public String getSuggestion() {
        return suggestion;
    }

}