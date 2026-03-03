package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkConfigDto {
    @Min(value = 1, message = "Warehouses must be at least 1")
    @Max(value = 10000, message = "Warehouses cannot exceed 10000")
    private Integer warehouses;

    @Min(value = 1, message = "Terminals must be at least 1")
    @Max(value = 1000, message = "Terminals cannot exceed 1000")
    private Integer terminals;

    @Min(value = 1, message = "Duration must be at least 1 second")
    @Max(value = 86400, message = "Duration cannot exceed 24 hours")
    private Integer duration;

    @Min(value = 0, message = "Rampup cannot be negative")
    @Max(value = 3600, message = "Rampup cannot exceed 1 hour")
    private Integer rampup;

    private Boolean thinkTime;

    @Min(value = 1, message = "Load concurrency must be at least 1")
    @Max(value = 100, message = "Load concurrency cannot exceed 100")
    private Integer loadConcurrency;

    private String loadMode;

    @Valid
    private TransactionMixDto mix;
}
