package com.ninedata.dbbench.web.exception;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    private boolean success;
    private String error;
    private String errorType;
    private String suggestion;
    private long timestamp;
    private String path;
}
