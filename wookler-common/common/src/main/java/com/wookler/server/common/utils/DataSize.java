/**
 * TODO: <comments>
 *
 * @file DataSize.java
 * @author subho
 * @date 23-Nov-2015
 */
package com.wookler.server.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Class representing the data size definition. This consists of the size value
 * and the size unit (ex. 1024TB, 2048 MB, etc.)
 *
 * @author subho
 * @date 23-Nov-2015
 */
public class DataSize {
    /** Regex pattern for data size string */
    private static final String REGEX = "(^\\d+)\\s*([a-zA-Z]*$)";

    /**
     * The Enum ESizeUnit -- represents the supported data size units (B, KB,
     * MB, GB, TB and PB)
     */
    public static enum ESizeUnit {
        B, KB, MB, GB, TB, PB;

        /**
         * Parses the size unit value string and returns the corresponding enum
         *
         * @param value
         *            the size unit value string
         * @return the enum value
         */
        public static ESizeUnit parse(String value) {
            value = value.toUpperCase();

            return ESizeUnit.valueOf(value);
        }
    }

    /** The source string from which data size needs to be constructed. */
    private String source;
    /** Data size value in bytes */
    private long value;
    /** Data size unit */
    private ESizeUnit unit;

    /**
     * Get the source string repsenting the DataSize
     * 
     * @return the source
     */
    public String getSource() {
        return source;
    }

    /**
     * Set the DataSize source string
     * 
     * @param source
     *            the source to set
     */
    public void setSource(String source) {
        this.source = source;
    }

    /**
     * Get the DataSize value in bytes
     * 
     * @return the value
     */
    public long getValue() {
        return value;
    }

    /**
     * Set the DataSize value in bytes
     * 
     * @param value
     *            the value to set
     */
    public void setValue(long value) {
        this.value = value;
    }

    /**
     * Get the DataSize size unit as {@link ESizeUnit}
     * 
     * @return the unit
     */
    public ESizeUnit getUnit() {
        return unit;
    }

    /**
     * Set the DataSize {@link ESizeUnit} unit
     * 
     * @param unit
     *            the unit to set
     */
    public void setUnit(ESizeUnit unit) {
        this.unit = unit;
    }

    /**
     * Parses the source string and get the corresponding DataSize object
     *
     * @param source
     *            the source string representing the data size
     * @return newly constructed {@link DataSize} instance
     */
    public static DataSize parse(String source) {
        if (!StringUtils.isEmpty(source)) {
            source = source.trim().toUpperCase();
            Pattern p = Pattern.compile(REGEX);
            Matcher m = p.matcher(source);
            while (m.find()) {
                String n = m.group(1);
                String u = m.group(2);
                if (!StringUtils.isEmpty(n)) {
                    DataSize ds = new DataSize();
                    ds.source = source;
                    ds.value = Long.parseLong(n);
                    if (!StringUtils.isEmpty(u)) {
                        ds.unit = ESizeUnit.parse(u);
                        if (ds.unit != null) {
                            switch (ds.unit) {
                            case KB:
                                ds.value = ds.value * 1024;
                                break;
                            case MB:
                                ds.value = ds.value * 1024 * 1024;
                                break;
                            case GB:
                                ds.value = ds.value * 1024 * 1024 * 1024;
                                break;
                            case TB:
                                ds.value = ds.value * 1024 * 1024 * 1024 * 1024;
                                break;
                            case PB:
                                ds.value = ds.value * 1024 * 1024 * 1024 * 1024 * 1024;
                                break;
                            case B:
                                break;
                            }
                        }
                    } else {
                        ds.unit = ESizeUnit.B;
                    }
                    return ds;
                }
            }
        }
        return null;
    }

    /**
     * String representation of DataSize object -- [source] value (unit) ex.
     * [1024TB] 1125899906842624 (TB)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("[%s] %d (%s)", source, value, unit.name());
    }

}
