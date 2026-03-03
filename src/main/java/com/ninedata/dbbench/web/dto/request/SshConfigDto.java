package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SshConfigDto {
    private Boolean enabled;
    private String host;

    @Min(value = 1, message = "Port must be at least 1")
    @Max(value = 65535, message = "Port cannot exceed 65535")
    private Integer port;

    private String username;
    private String password;
    private String privateKey;
}
