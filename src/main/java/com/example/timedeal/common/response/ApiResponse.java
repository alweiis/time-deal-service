package com.example.timedeal.common.response;

import lombok.Builder;
import lombok.Getter;
import org.slf4j.MDC;

import java.time.LocalDateTime;

@Getter
public class ApiResponse<T> {
    private final LocalDateTime timestamp;
    private final String traceId;
    private final String code;
    private final String message;
    private final T data;

    @Builder
    private ApiResponse(String code, String message, T data) {
        this.timestamp = LocalDateTime.now();
        this.traceId = MDC.get("traceId");
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static  <T> ApiResponse<T> success(T data) {
        return ApiResponse.<T>builder()
                .code("SUCCESS")
                .message("요청이 성공적으로 처리되었습니다")
                .data(data)
                .build();
    }
}
