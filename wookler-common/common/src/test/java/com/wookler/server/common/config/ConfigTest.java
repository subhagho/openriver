/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.common.config;

import java.lang.reflect.Field;

import com.wookler.server.common.utils.ReflectionUtils;

import junit.framework.TestCase;

public class ConfigTest extends TestCase {
	public static enum ETest {
		TEST1, TEST2, TEST3
	}

	@CPath(path = "test.auto")
	public static final class TestAuto {
		@CParam(name = "@class")
		private String	classname;
		@CParam(name = "@autoload")
		private boolean	autoload;
		@CParam(name = "enum")
		private ETest	evalue;
		@CParam(name = "double")
		private Double	dvalue;

		/**
		 * @return the classname
		 */
		public String getClassname() {
			return classname;
		}

		/**
		 * @param classname
		 *            the classname to set
		 */
		public void setClassname(String classname) {
			this.classname = classname;
		}

		/**
		 * @return the autoload
		 */
		public boolean isAutoload() {
			return autoload;
		}

		/**
		 * @param autoload
		 *            the autoload to set
		 */
		public void setAutoload(boolean autoload) {
			this.autoload = autoload;
		}

		/**
		 * @return the evalue
		 */
		public ETest getEvalue() {
			return evalue;
		}

		/**
		 * @param evalue
		 *            the evalue to set
		 */
		public void setEvalue(ETest evalue) {
			this.evalue = evalue;
		}

		/**
		 * @return the dvalue
		 */
		public Double getDvalue() {
			return dvalue;
		}

		/**
		 * @param dvalue
		 *            the dvalue to set
		 */
		public void setDvalue(Double dvalue) {
			this.dvalue = dvalue;
		}
	}

	private static final String	CONFIG_FILE			= "src/test/resources/test-config.xml";
	private static final String	CONFIG_ROOT_PATH	= "/configuration/river";
	private static final String	CONFIG_FILE_AUTO	= "src/test/resources/test-auto-config.xml";

	public void testLoad() throws Exception {
		Config config = new Config(CONFIG_FILE, CONFIG_ROOT_PATH);
		ConfigParser parser = new XMLConfigParser();
		parser.parse(config, CONFIG_FILE, CONFIG_ROOT_PATH);
		System.out.println(config.toString());
	}

	public void testAutoParse() throws Exception {
		Config config = new Config(CONFIG_FILE_AUTO, CONFIG_ROOT_PATH);
        ConfigParser parser = new XMLConfigParser();
        parser.parse(config, CONFIG_FILE, CONFIG_ROOT_PATH);

		TestAuto ta = new TestAuto();
		ConfigUtils.parse(config.node(), ta);

		Field[] fields = ReflectionUtils.getAllFields(ta.getClass());
		for (Field f : fields) {
			String s = ReflectionUtils.strinfigy(ta, f);
			System.out.println("Field:" + f.getName() + ", Value:" + s);
		}
	}
}