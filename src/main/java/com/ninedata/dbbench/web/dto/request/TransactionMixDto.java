package com.ninedata.dbbench.web.dto.request;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransactionMixDto {
    @Min(value = 0, message = "New order percentage cannot be negative")
    @Max(value = 100, message = "New order percentage cannot exceed 100")
    private Integer newOrder;

    @Min(value = 0, message = "Payment percentage cannot be negative")
    @Max(value = 100, message = "Payment percentage cannot exceed 100")
    private Integer payment;

    @Min(value = 0, message = "Order status percentage cannot be negative")
    @Max(value = 100, message = "Order status percentage cannot exceed 100")
    private Integer orderStatus;

    @Min(value = 0, message = "Delivery percentage cannot be negative")
    @Max(value = 100, message = "Delivery percentage cannot exceed 100")
    private Integer delivery;

    @Min(value = 0, message = "Stock level percentage cannot be negative")
    @Max(value = 100, message = "Stock level percentage cannot exceed 100")
    private Integer stockLevel;
}
