package com.adc.batch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.WordUtils;

/**
 *
 * @author Debopam
 */
public class TableBeanMapping {

    public static void generateBean(String folderName) {
        try {
            Connection connection = OracleJDBC.GetConnection();
            ColumnDatatypeMapping cdm;
            String sqlSelectTableName = "SELECT TABLE_NAME FROM USER_TABLES";
            PreparedStatement psSelectTable = connection.prepareStatement(sqlSelectTableName);
            ResultSet rsTable = psSelectTable.executeQuery();
            String tableName;
            while (rsTable.next()) {
                cdm = new ColumnDatatypeMapping();
                tableName = rsTable.getString("TABLE_NAME");
                String sqlSelectColumn = 
                "SELECT COLUMN_NAME FROM COLS WHERE TABLE_NAME='" + 
                tableName + "'";
                PreparedStatement psSelectColumn = connection.prepareStatement(sqlSelectColumn);
                ResultSet rsColumn = psSelectColumn.executeQuery();
                String columnName;
                while (rsColumn.next()) {
                    columnName = rsColumn.getString("COLUMN_NAME");
                    String sqlSelectDatatype = "SELECT DATA_TYPE FROM COLS WHERE TABLE_NAME='" + tableName + "' AND COLUMN_NAME='" +columnName + "'";
                    PreparedStatement psSelectDatatype = connection.prepareStatement(sqlSelectDatatype);
                    ResultSet rsDatatype = psSelectDatatype.executeQuery();
                    String datatypeName;
                    while (rsDatatype.next()) {
                        datatypeName = rsDatatype.getString("DATA_TYPE");
                        cdm.put(columnName, datatypeName);
                    }
                    psSelectDatatype.close();
                    rsDatatype.close();
                }
                psSelectColumn.close();
                rsColumn.close();
                writeBean(folderName, tableName, cdm);
            }
            psSelectTable.close();
            rsTable.close();
        } catch (SQLException ex) {
            Logger.getLogger(TableBeanMapping.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void writeBean(String folderName, String tableName, ColumnDatatypeMapping cdm) {
        FileWriter fileWriter;
        BufferedWriter bufferedWriter;
        StringBuilder fileContent;
        String className = getConventionalClassName(tableName);
        SQLJavaDatatypeMapping sqlJavaDatatypeMapping = new SQLJavaDatatypeMapping();
        try {
            fileWriter = new FileWriter(folderName + "\\" + className + ".java");
            bufferedWriter = new BufferedWriter(fileWriter);
            fileContent = new StringBuilder();
            fileContent.append("/*");
            fileContent.append("\n* File\t\t: ").append(className).append(".java");
            fileContent.append("\n* Date Created\t: ").append(Calendar.getInstance().getTime().toString());
            fileContent.append("\n*/");
            fileContent.append("\n\n");
            fileContent.append("public class ").append(className).append(" {");
            fileContent.append("\n");
            for (ColumnDatatypeEntry entry : cdm.entrySet()) {
                fileContent.append("\n\t");
                fileContent.append("private");
                fileContent.append(" ");
                fileContent.append(sqlJavaDatatypeMapping.getJavaDatatype(entry.getDatatype()));
                fileContent.append(" ");
                fileContent.append(entry.getColumn().toLowerCase());
                fileContent.append(";");
            }

            for (ColumnDatatypeEntry entry : cdm.entrySet()) {
                fileContent.append("\n\n\t");
                fileContent.append("public");
                fileContent.append(" ");
                fileContent.append(sqlJavaDatatypeMapping.getJavaDatatype(entry.getDatatype()));
                fileContent.append(" ");
                fileContent.append(getAccessorMethodName(entry.getColumn().toLowerCase()));
                fileContent.append("() {");
                fileContent.append("\n\t\t").append("return ").append
                (entry.getColumn().toLowerCase()).append(";");
                fileContent.append("\n\t").append("}");

                fileContent.append("\n\n\t");
                fileContent.append("public");
                fileContent.append(" ");
                fileContent.append("void");
                fileContent.append(" ");
                fileContent.append(getMutatorMethodName(entry.getColumn().toLowerCase()));
                fileContent.append("(");
                fileContent.append(sqlJavaDatatypeMapping.getJavaDatatype(entry.getDatatype()));
                fileContent.append(" ").append(entry.getColumn().toLowerCase());
                fileContent.append(") {");
                fileContent.append("\n\t\t").append("this.").append
                (entry.getColumn().toLowerCase()).append(" = ").append
                (entry.getColumn().toLowerCase()).append(";");
                fileContent.append("\n\t").append("}");
            }
            fileContent.append("\n").append("}");

            bufferedWriter.write(fileContent.toString());
            bufferedWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(TableBeanMapping.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String getConventionalClassName(String str) {
        String conventionalClassName = "";
        String[] splittedStr = str.split("[_]");
        for (int i = 0; i < splittedStr.length; i++) {
            conventionalClassName += WordUtils.capitalizeFully(splittedStr[i]);
        }
        return conventionalClassName;
    }

    public static String getConventionalMethodName(String str) {
        String conventionalClassName = getConventionalClassName(str);
        return Character.toLowerCase(conventionalClassName.charAt(0)) + 
        conventionalClassName.substring(1);
    }

    public static String getAccessorMethodName(String dataMemberName) {
        return "get" + Character.toUpperCase(dataMemberName.charAt(0)) + 
        dataMemberName.substring(1);
    }

    public static String getMutatorMethodName(String dataMemberName) {
        return "set" + Character.toUpperCase(dataMemberName.charAt(0)) + 
        dataMemberName.substring(1);
    }
}