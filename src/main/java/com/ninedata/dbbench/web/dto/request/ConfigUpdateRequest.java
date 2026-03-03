package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConfigUpdateRequest {
    @Valid
    private DatabaseConfigDto database;

    @Valid
    private BenchmarkConfigDto benchmark;
}
