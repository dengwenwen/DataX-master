package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HiveReaderErrorCode implements ErrorCode {
    KERBEROS_LOGIN_ERROR("HiveReader-13", "KERBEROS认证失败"),
    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","参数不合法"),
    CLASS_EXCEPTION("ILLEGAL_CALSS", "获取操作类失败"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS","不合法的hive jdbc地址"),
    EXECUTE_EXCEPTION("EXECUTE_EXCEPTION", "sql执行错误"),
    JSONCAST_EXCEPTION("JSONCAST_EXCEPTION","json类型转换异常"),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION","未知异常");


    private final String code;

    private final String description;

    private HiveReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}