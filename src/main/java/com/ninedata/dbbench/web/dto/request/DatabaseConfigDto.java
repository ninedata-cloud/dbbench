package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseConfigDto {
    @NotBlank(message = "Database type is required")
    private String type;

    @NotBlank(message = "JDBC URL is required")
    @Pattern(regexp = "^jdbc:.*", message = "Invalid JDBC URL format")
    private String jdbcUrl;

    private String username;

    private String password;

    @Valid
    private PoolConfigDto pool;

    @Valid
    private SshConfigDto ssh;
}
