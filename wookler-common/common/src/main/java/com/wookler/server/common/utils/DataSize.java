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
 * TODO: <comment>
 *
 * @author subho
 * @date 23-Nov-2015
 */
public class DataSize {
	private static final String REGEX = "(^\\d+)\\s*([a-zA-Z]*$)";

	public static enum ESizeUnit {
		B, KB, MB, GB, TB, PB;

		public static ESizeUnit parse(String value) {
			value = value.toUpperCase();

			return ESizeUnit.valueOf(value);
		}
	}

	private String		source;
	private long		value;
	private ESizeUnit	unit;

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * @return the value
	 */
	public long getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(long value) {
		this.value = value;
	}

	/**
	 * @return the unit
	 */
	public ESizeUnit getUnit() {
		return unit;
	}

	/**
	 * @param unit
	 *            the unit to set
	 */
	public void setUnit(ESizeUnit unit) {
		this.unit = unit;
	}

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
									ds.value = ds.value * 1024 * 1024 * 1024
											* 1024;
									break;
								case PB:
									ds.value = ds.value * 1024 * 1024 * 1024
											* 1024 * 1024;
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("[%s] %d (%s)", source, value, unit.name());
	}

}
