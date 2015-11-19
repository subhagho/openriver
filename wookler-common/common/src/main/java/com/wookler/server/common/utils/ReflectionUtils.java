/*
 * Copyright 2014 Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wookler.server.common.utils;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import com.google.common.base.Preconditions;
import com.wookler.server.common.ConfigurationException;

/**
 * Publisher handle to the Message Queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * 
 *         11:10:30 AM
 *
 */
public class ReflectionUtils {
	public static final Field[] getAllFields(Class<?> type) {
		Preconditions.checkArgument(type != null);
		List<Field> fields = new ArrayList<>();
		getFields(type, fields);
		if (fields != null && !fields.isEmpty()) {
			Field[] fa = new Field[fields.size()];
			for (int ii = 0; ii < fields.size(); ii++) {
				fa[ii] = fields.get(ii);
			}
			return fa;
		}
		return null;
	}

	private static void getFields(Class<?> type, List<Field> fields) {
		Field[] fs = type.getDeclaredFields();
		if (fs != null && fs.length > 0) {
			for (Field f : fs) {
				if (f != null)
					fields.add(f);
			}
		}
		Class<?> st = type.getSuperclass();
		if (st != null && !st.equals(Object.class)) {
			getFields(st, fields);
		}
	}

	public static String strinfigy(Object o, Field field) throws Exception {
		Object v = getFieldValue(o, field);
		if (v != null) {
			return String.valueOf(v);
		}
		return null;
	}

	public static Object getFieldValue(Object o, Field field) throws Exception {
		String method = "get" + StringUtils.capitalize(field.getName());

		Method m = MethodUtils.getAccessibleMethod(o.getClass(), method);
		if (m == null) {
			method = field.getName();
			m = MethodUtils.getAccessibleMethod(o.getClass(), method);
		}

		if (m == null) {
			Class<?> type = field.getType();
			if (type.equals(boolean.class) || type.equals(Boolean.class)) {
				method = "is" + StringUtils.capitalize(field.getName());
				m = MethodUtils.getAccessibleMethod(o.getClass(), method);
			}
		}

		if (m == null)
			throw new Exception("No accessable method found for field. [field="
					+ field.getName() + "][class="
					+ o.getClass().getCanonicalName() + "]");
		return MethodUtils.invokeMethod(o, method);
	}

	public static boolean canStringify(Field field) {
		Preconditions.checkArgument(field != null);
		if (field.isEnumConstant() || field.getType().isEnum())
			return true;
		if (isPrimitiveTypeOrClass(field))
			return true;
		if (field.getType().equals(String.class))
			return true;
		if (field.getType().equals(Date.class))
			return true;
		return false;
	}

	public static final void setPrimitiveValue(String value, Object source,
			Field f) throws Exception {
		Class<?> type = f.getType();
		if (type.equals(boolean.class) || type.equals(Boolean.class)) {
			setBooleanValue(source, f, value);
		} else if (type.equals(short.class) || type.equals(Short.class)) {
			setShortValue(source, f, value);
		} else if (type.equals(int.class) || type.equals(Integer.class)) {
			setIntValue(source, f, value);
		} else if (type.equals(float.class) || type.equals(Float.class)) {
			setFloatValue(source, f, value);
		} else if (type.equals(double.class) || type.equals(Double.class)) {
			setDoubleValue(source, f, value);
		} else if (type.equals(long.class) || type.equals(Long.class)) {
			setLongValue(source, f, value);
		} else if (type.equals(char.class) || type.equals(Character.class)) {
			setCharValue(source, f, value);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static final Object setValueFromString(String value, Object source,
			Field f) throws ConfigurationException {
		Preconditions.checkArgument(!StringUtils.isEmpty(value));
		try {
			Object retV = value;
			Class<?> type = f.getType();
			if (ReflectionUtils.isPrimitiveTypeOrClass(f)) {
				ReflectionUtils.setPrimitiveValue(value, source, f);
			} else if (type.equals(String.class)) {
				ReflectionUtils.setStringValue(source, f, value);
			} else if (type.isEnum()) {
				Class<Enum> et = (Class<Enum>) type;
				Object ev = Enum.valueOf(et, value);
				ReflectionUtils.setObjectValue(source, f, ev);
				retV = ev;
			} else if (type.equals(File.class)) {
				File file = new File(value);
				ReflectionUtils.setObjectValue(source, f, file);
				retV = file;
			} else if (type.equals(Class.class)) {
				Class<?> cls = Class.forName(value.trim());
				ReflectionUtils.setObjectValue(source, f, cls);
				retV = cls;
			} else {
				Class<?> cls = Class.forName(value.trim());
				if (type.isAssignableFrom(cls)) {
					Object o = cls.newInstance();
					ReflectionUtils.setObjectValue(source, f, o);
					retV = o;
				} else {
					throw new InstantiationException(
							"Cannot create instance of type [type="
									+ cls.getCanonicalName()
									+ "] and assign to field [field="
									+ f.getName() + "]");
				}
			}
			return retV;
		} catch (Exception e) {
			throw new ConfigurationException(
					"Error setting object value : [type="
							+ source.getClass().getCanonicalName() + "][field="
							+ f.getName() + "]",
					e);
		}
	}

	public static void setObjectValue(Object o, Field f, Object value)
			throws Exception {
		String method = "set" + StringUtils.capitalize(f.getName());
		Method m = MethodUtils.getAccessibleMethod(o.getClass(), method,
				f.getType());
		if (m == null) {
			method = f.getName();
			m = MethodUtils.getAccessibleMethod(o.getClass(), method,
					f.getType());
		}

		if (m == null)
			throw new Exception("No accessable method found for field. [field="
					+ f.getName() + "][class=" + o.getClass().getCanonicalName()
					+ "]");
		MethodUtils.invokeMethod(o, method, value);
	}

	public static void setStringValue(Object o, Field f, String value)
			throws Exception {
		setObjectValue(o, f, value);
	}

	public static void setBooleanValue(Object o, Field f, String value)
			throws Exception {
		boolean bv = Boolean.valueOf(value);
		setObjectValue(o, f, bv);
	}

	public static void setShortValue(Object o, Field f, String value)
			throws Exception {
		short sv = Short.parseShort(value);
		setObjectValue(o, f, sv);
	}

	public static void setIntValue(Object o, Field f, String value)
			throws Exception {
		int iv = Integer.parseInt(value);
		setObjectValue(o, f, iv);
	}

	public static void setLongValue(Object o, Field f, String value)
			throws Exception {
		long lv = Long.parseLong(value);
		setObjectValue(o, f, lv);
	}

	public static void setFloatValue(Object o, Field f, String value)
			throws Exception {
		float fv = Float.parseFloat(value);
		setObjectValue(o, f, fv);
	}

	public static void setDoubleValue(Object o, Field f, String value)
			throws Exception {
		double dv = Double.parseDouble(value);
		setObjectValue(o, f, dv);
	}

	public static void setCharValue(Object o, Field f, String value)
			throws Exception {
		char cv = value.charAt(0);
		setObjectValue(o, f, cv);
	}

	public static final boolean isPrimitiveTypeOrClass(Field field) {
		Class<?> type = field.getType();
		if (type.isPrimitive())
			return true;
		else if (type.equals(Boolean.class) || type.equals(Short.class)
				|| type.equals(Integer.class) || type.equals(Long.class)
				|| type.equals(Float.class) || type.equals(Double.class)
				|| type.equals(Character.class)) {
			return true;
		}
		return false;
	}
}
