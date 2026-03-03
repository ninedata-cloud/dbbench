package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SshTestRequest {
    @Valid
    private SshConfigDto ssh;

    private DatabaseConfigDto database;
}
