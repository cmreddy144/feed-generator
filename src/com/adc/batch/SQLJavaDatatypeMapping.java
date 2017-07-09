package com.adc.batch;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Debopam
 */
public class SQLJavaDatatypeMapping {

    private Map<String, String> sqlJavaDatatypeMapping;

    /*
     * In this mapping:
     * key      : SQL Datatype / JDBC Datatype
     * value    : Std. Java Datatype
     */
    public SQLJavaDatatypeMapping() {
        sqlJavaDatatypeMapping = new HashMap();

        /* SQL Datatypes to Std. Java Types */
        sqlJavaDatatypeMapping.put("CHAR", "String");
        sqlJavaDatatypeMapping.put("VARCHAR2", "String");
        sqlJavaDatatypeMapping.put("LONG", "String");
        sqlJavaDatatypeMapping.put("NUMBER", "int");
        sqlJavaDatatypeMapping.put("RAW", "byte[]");
        sqlJavaDatatypeMapping.put("LONGRAW", "byte[]");
        sqlJavaDatatypeMapping.put("DATE", "java.sql.Date");
        sqlJavaDatatypeMapping.put("BLOB", "java.sql.Blob");
        sqlJavaDatatypeMapping.put("CLOB", "java.sql.Clob");

        /* JDBC Datatypes to Std. Java Types */
        sqlJavaDatatypeMapping.put("VARCHAR", "String");
        sqlJavaDatatypeMapping.put("LONGVARCHAR", "String");
        sqlJavaDatatypeMapping.put("NUMERIC", "java.math.BigDecimal");
        sqlJavaDatatypeMapping.put("DECIMAL", "byte[]");
        sqlJavaDatatypeMapping.put("BIT", "boolean");
        sqlJavaDatatypeMapping.put("TINYINT", "byte");
        sqlJavaDatatypeMapping.put("SMALLINT", "short");
        sqlJavaDatatypeMapping.put("INTEGER", "int");
        sqlJavaDatatypeMapping.put("BIGINT", "long");
        sqlJavaDatatypeMapping.put("REAL", "float");
        sqlJavaDatatypeMapping.put("FLOAT", "double");
        sqlJavaDatatypeMapping.put("DOUBLE", "double");
        sqlJavaDatatypeMapping.put("BINARY", "byte[]");
        sqlJavaDatatypeMapping.put("VARBINARY", "byte[]");
        sqlJavaDatatypeMapping.put("LONGVARBINARY", "byte[]");
        sqlJavaDatatypeMapping.put("TIME", "java.sql.Time");
        sqlJavaDatatypeMapping.put("TIMESTAMP", "java.sql.Timestamp");
        sqlJavaDatatypeMapping.put("STRUCT", "java.sql.Struct");
        sqlJavaDatatypeMapping.put("REF", "java.sql.Ref");
        sqlJavaDatatypeMapping.put("ARRAY", "java.sql.Array");
    }

    public String getJavaDatatype(String sqlDatatype) {
        String javaDatatype = null;

        for (Map.Entry<String, String> entry : sqlJavaDatatypeMapping.entrySet()) {
            sqlDatatype = sqlDatatype.replaceAll("[\\d()]", "");
            if (entry.getKey().equals(sqlDatatype.toUpperCase())) {
                javaDatatype = entry.getValue();
            }
        }

        return javaDatatype;
    }
}
