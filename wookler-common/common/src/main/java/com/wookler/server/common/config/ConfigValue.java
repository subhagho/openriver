/*
 * Copyright 2014 Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.common.config;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.GlobalConstants;

import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Value node in a configuration tree.
 * <p/>
 *
 * @author subghosh
 * @createdt 15/02/14.
 */
public class ConfigValue extends AbstractConfigNode {
	private static final Pattern pattern = Pattern.compile("\\$\\{.*?\\}");

	private String name;
	private String value;
	private boolean parsed = false;

	/**
	 * Constructor with name and value.
	 *
	 * @param name
	 *            - Node name
	 * @param value
	 *            - Node value.
	 */
	public ConfigValue(String name, String value, ConfigNode parent,
			Config owner) {
		super((AbstractConfigNode) parent, owner);
		this.name = name;
		this.value = value;
	}

	/**
	 * Get the name of this node.
	 *
	 * @return - Node name.
	 */
	public String name() {
		return name;
	}

	/**
	 * Set the name of this configuration node.
	 *
	 * @param name
	 *            - Node name.
	 * @return - Self.
	 */
	@Override
	public ConfigNode name(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Get the value of this configuration node.
	 *
	 * @return - String value.
	 */
	public String value() {
		if (owner.state() == EObjectState.Available) {
			if (!parsed) {
				Matcher m = pattern.matcher(value);
				StringBuffer sb = new StringBuffer();
				int indx = 0;
				while (m.find()) {
					String key = this.value.substring(m.start(), m.end());
					if (StringUtils.isEmpty(key))
						break;
					String k = key.replace("$", "").replace("{", "")
							.replace("}", "").trim();
					String v = owner.property(k);
					if (v == null) {
						v = System.getProperty(k);
					}
					if (v == null) {
						v = System.getenv(k);
					}
					if (v != null) {
						sb.append(this.value.substring(indx, m.start()))
								.append(v);
						indx = m.end();
						parsed = true;
					}
				}
				if (indx < this.value.length() - 1) {
					sb.append(this.value.substring(indx));
				}
				this.value = sb.toString();
			}
		}
		return value;
	}

	/**
	 * Set the value of this configuration node.
	 *
	 * @param value
	 *            - String value.
	 * @return - self.
	 */
	public ConfigValue value(String value) {
		this.value = value;
		return this;
	}

	/**
	 * Is this a leaf node in the configuration tree. Always true.
	 *
	 * @return - Is leaf?
	 */
	@Override
	public boolean isLeaf() {
		return true;
	}

	/**
	 * Get the value string parsed as an integer.
	 *
	 * @return - Integer value.
	 */
	public int getIntValue() {
		return Integer.parseInt(value);
	}

	/**
	 * Get the value string parsed as an integer.
	 *
	 * @param def
	 *            - Default value.
	 * @return - Integer value.
	 */
	public int getIntValue(int def) {
		try {
			if (!StringUtils.isEmpty(value))
				return Integer.parseInt(value);
		} catch (Exception ex) {
			// Do nothing.
		}
		return def;
	}

	/**
	 * Get the value string parsed as an long.
	 *
	 * @return - Long value.
	 */
	public long getLongValue() {
		return Long.parseLong(value);
	}

	/**
	 * Get the value string parsed as an long.
	 *
	 * @param def
	 *            - Default value.
	 * @return - Long value.
	 */
	public long getLongValue(long def) {
		try {
			if (!StringUtils.isEmpty(value))
				return Long.parseLong(value);
		} catch (Exception ex) {
			// Do nothing.
		}
		return def;
	}

	/**
	 * Get the value string parsed as an double.
	 *
	 * @return - Long value.
	 */
	public double getDoubleValue() {
		return Double.parseDouble(value);
	}

	/**
	 * Get the value string parsed as an double.
	 *
	 * @param def
	 *            - Default value.
	 * @return - Double value.
	 */
	public double getDoubleValue(double def) {
		try {
			if (!StringUtils.isEmpty(value))
				return Double.parseDouble(value);
		} catch (Exception ex) {
			// Do nothing.
		}
		return def;
	}

	/**
	 * Get the value string as a Date.
	 *
	 * @return - Date value.
	 * @throws com.wookler.server.common.ConfigurationException
	 */
	public Date getDateValue() throws ConfigurationException {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(
					GlobalConstants.DefaultDateTimeFormat);
			return sdf.parse(value);
		} catch (ParseException pe) {
			throw new ConfigurationException(
					"Error parsing value to date. [value=" + value + "]", pe);
		}
	}

	/**
	 * Get the value string as a Date.
	 *
	 * @param format
	 *            - Date format to parse the value with.
	 * @return - Date value.
	 * @throws ConfigurationException
	 */
	public Date getDateValue(String format) throws ConfigurationException {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.parse(value);
		} catch (ParseException pe) {
			throw new ConfigurationException(
					"Error parsing value to date. [value=" + value + "]", pe);
		}
	}

	public boolean getBooleanValue() throws ConfigurationException {
		String v = value();
		if (!StringUtils.isEmpty(v)) {
			boolean b = false;
			if (v.compareToIgnoreCase("true") == 0 || v.compareToIgnoreCase("yes") == 0 || v.compareTo("1") == 0) {
				b = true;
			} else {
				b = false;
			}
			return b;
		} else {
			throw new ConfigurationException("Error parsing boolean value : Value is NULL/empty.");
		}
	}
	/**
	 * Create a copy of this node.
	 *
	 * @return - Copy of node.
	 */
	@Override
	public ConfigNode copy() {
		return new ConfigValue(name, value, parent(), owner);
	}

	/**
	 * Default string representation of this instance.
	 *
	 * @return - String representation.
	 */
	@Override
	public String toString() {
		return "name='" + name + '\'' + ", value='" + value() + '\'';
	}
}
