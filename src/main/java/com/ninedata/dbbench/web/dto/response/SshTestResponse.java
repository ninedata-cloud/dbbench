package com.ninedata.dbbench.web.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SshTestResponse {
    private boolean success;
    private String message;
    private String host;
    private Integer port;
    private Long responseTime;
    private String error;
}
