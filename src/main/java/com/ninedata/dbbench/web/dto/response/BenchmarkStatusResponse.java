package com.ninedata.dbbench.web.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkStatusResponse {
    private String status;
    private boolean running;
    private boolean loading;
    private Integer loadProgress;
    private String loadMessage;
}
