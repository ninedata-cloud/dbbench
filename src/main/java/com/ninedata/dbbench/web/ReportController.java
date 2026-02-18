package com.ninedata.dbbench.web;

import com.ninedata.dbbench.report.ReportGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/api/report")
@RequiredArgsConstructor
public class ReportController {
    private final ReportGenerator reportGenerator;

    @GetMapping(value = "/markdown", produces = "text/markdown; charset=UTF-8")
    public ResponseEntity<String> getMarkdown() {
        String markdown = reportGenerator.generateMarkdown();
        return ResponseEntity.ok(markdown);
    }

    @GetMapping(value = "/download/markdown", produces = "text/markdown; charset=UTF-8")
    public ResponseEntity<byte[]> downloadMarkdown() {
        String markdown = reportGenerator.generateMarkdown();
        String filename = "dbbench-report-" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".md";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(new MediaType("text", "markdown", StandardCharsets.UTF_8))
                .body(markdown.getBytes(StandardCharsets.UTF_8));
    }
}
