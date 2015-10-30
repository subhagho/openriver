/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.common.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import com.google.common.base.Preconditions;

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
		if (field.getType().isPrimitive())
			return true;
		if (field.getType().equals(String.class))
			return true;
		return false;
	}
}
