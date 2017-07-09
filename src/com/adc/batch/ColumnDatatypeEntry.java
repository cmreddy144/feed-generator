package com.adc.batch;

/**
 *
 * @author Debopam
 */
public class ColumnDatatypeEntry {

    private final String column;
    private String datatype;

    public ColumnDatatypeEntry(String column, String datatype) {
        this.column = column;
        this.datatype = datatype;
    }

    /**
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    /**
     * @return the datatype
     */
    public String getDatatype() {
        return datatype;
    }

    /**
     * @param datatype the datatype to set
     */
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
}
