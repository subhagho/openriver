/**
 * TODO: <comments>
 *
 * @file Filename.java
 * @author subho
 * @date 24-Nov-2015
 */
package com.wookler.server.common.utils;

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.wookler.server.common.Env;
import com.wookler.server.common.config.Config;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 24-Nov-2015
 */
public class PropertySubstitute {
	private static final String REGEX = "(\\$\\{\\w+\\})";

	public static final String substitute(String format, Object source,
			boolean useEnv) throws Exception {
		Preconditions.checkArgument(!StringUtils.isEmpty(format));

		String name = format;
		Pattern p = Pattern.compile(REGEX);
		Matcher m = p.matcher(format);

		while (m.find()) {
			if (m.groupCount() > 0) {
				String v = m.group(0);
				if (!StringUtils.isEmpty(v)) {
					String vn = getVarName(v);
					if (!StringUtils.isEmpty(vn)) {
						String value = getVarValue(vn, source, useEnv);
						if (!StringUtils.isEmpty(value)) {
							name = name.replaceAll(escape(v), value);
						}
					}
				}
			}
		}

		return name;
	}

	public static final String escape(String input) {
		StringBuffer b = new StringBuffer();
		if (!StringUtils.isEmpty(input)) {
			char[] str = input.toCharArray();
			for (char c : str) {
				switch (c) {
					case '$':
						b.append("\\$");
						break;
					case '{':
						b.append("\\{");
						break;
					case '}':
						b.append("\\}");
						break;
					default:
						b.append(c);
				}
			}
		}
		return b.toString();
	}

	private static final String getVarValue(String var, Object source,
			boolean env) throws Exception {
		if (source != null) {
			Class<?> type = source.getClass();
			Field[] fields = ReflectionUtils.getAllFields(type);
			if (fields != null && fields.length > 0) {
				for (Field field : fields) {
					if (field.getName().compareTo(var) == 0) {
						String value = String.valueOf(
								ReflectionUtils.getFieldValue(source, field));
						if (!StringUtils.isEmpty(value)) {
							return value;
						}
						break;
					}
				}
			}
		}
		if (env) {
			Config config = Env.get().config();
			if (config != null) {
				String value = config.property(var);
				if (!StringUtils.isEmpty(value))
					return value;
			}
			String value = System.getProperty(var);
			if (!StringUtils.isEmpty(value))
				return value;
		}
		return null;
	}

	private static final String getVarName(String input) {
		int s = input.indexOf('{');
		int e = input.indexOf('}');
		if (s >= 0 && e > s) {
			return input.substring(s + 1, e);
		}
		return null;
	}
}
