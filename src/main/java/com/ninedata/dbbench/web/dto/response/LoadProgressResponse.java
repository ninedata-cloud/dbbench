package com.ninedata.dbbench.web.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadProgressResponse {
    private boolean loading;
    private Integer progress;
    private String message;
    private String status;
}
