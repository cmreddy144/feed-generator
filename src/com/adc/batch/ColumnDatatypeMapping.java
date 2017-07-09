package com.adc.batch;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Debopam
 */
public class ColumnDatatypeMapping {

    private int size;
    private int DEFAULT_CAPACITY = 16;
    private ColumnDatatypeEntry[] values = new ColumnDatatypeEntry[DEFAULT_CAPACITY];

    public String get(String column) {
        for (int i = 0; i < size; i++) {
            if (values[i] != null) {
                if (values[i].getColumn().equals(column)) {
                    return values[i].getDatatype();
                }
            }
        }
        return null;
    }

    public void put(String column, String datatype) {
        boolean flag = true;
        for (int i = 0; i < size; i++) {
            if (values[i].getColumn().equals(column)) {
                values[i].setDatatype(datatype);
                flag = false;
            }
        }
        if (flag) {
            manageCapacity();
            values[size++] = new ColumnDatatypeEntry(column, datatype);
        }
    }

    private void manageCapacity() {
        if (size == values.length) {
            int newSize = values.length * 2;
            values = Arrays.copyOf(values, newSize);
        }
    }

    public int size() {
        return size;
    }

    public void remove(String column) {
        for (int i = 0; i < size; i++) {
            if (values[i].getColumn().equals(column)) {
                values[i] = null;
                size--;
                condenseArray(i);
            }
        }
    }

    private void condenseArray(int start) {
        for (int i = start; i < size - 1; i++) {
            values[i] = values[i + 1];
        }
    }

    public Set<String> columnSet() {
        Set<String> columnSet = new HashSet<String>();
        for (int i = 0; i < size; i++) {
            columnSet.add(values[i].getColumn());
        }
        return columnSet;
    }

    public Set<ColumnDatatypeEntry> entrySet() {
        Set<ColumnDatatypeEntry> entrySet = new HashSet<ColumnDatatypeEntry>();
        for (int i = 0; i < size; i++) {
            entrySet.add(values[i]);
        }
        return entrySet;
    }
}
