package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PoolConfigDto {
    @Min(value = 1, message = "Pool size must be at least 1")
    @Max(value = 1000, message = "Pool size cannot exceed 1000")
    private Integer size;

    @Min(value = 0, message = "Min idle cannot be negative")
    @Max(value = 1000, message = "Min idle cannot exceed 1000")
    private Integer minIdle;
}
