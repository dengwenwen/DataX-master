package com.alibaba.datax.plugin.reader.hivereader;

/**
 * @auth: ronghua.yu
 * @time: 16/11/2
 * @desc:
 */
public enum ColumnType {
    TINYINT("tinyint"),
    SMALLINT("smallint"),
    INT("int"),
    BIGINT("bigint"),
    BOOLEAN("boolean"),
    FLOAT("float"),
    DOUBLE("double"),
    STRING("string"),
    TIMESTAMP("timestamp"),
    DECIMAL("decimal"),
    CHAR("char"),
    VARCHAR("varchar"),
    DATE("date"),
    NULL("null"),
    ARRAY("array"),
    OBJECT("object");

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        for (ColumnType columnType : values()) {
            if (columnType.typeName.equalsIgnoreCase(typeName)) {
                return columnType;
            }

        }
        return STRING;
        //throw DataException.asDataException(HiveReaderErrorCode.ILLEGAL_VALUE,
        //        String.format("hive 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }

    private String typeName;
}