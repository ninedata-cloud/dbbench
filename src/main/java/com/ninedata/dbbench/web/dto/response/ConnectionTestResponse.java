package com.ninedata.dbbench.web.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionTestResponse {
    private boolean success;
    private String message;
    private String database;
    private String jdbcUrl;
    private Long responseTime;
    private String error;
    private String errorType;
    private String suggestion;
}
