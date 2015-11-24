/**
 * TODO: <comments>
 *
 * @file Test_DataSize.java
 * @author subho
 * @date 23-Nov-2015
 */
package com.wookler.server.common.utils;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 23-Nov-2015
 */
public class Test_DataSize {

	/**
	 * Test method for {@link com.wookler.server.common.utils.DataSize#parse(java.lang.String)}.
	 */
	@Test
	public void testParse() {
		try {
			String v = "1024TB";
			DataSize ds = DataSize.parse(v);
			System.out.println(ds);
			
			v = "1423 MB";
			ds = DataSize.parse(v);
			System.out.println(ds);
			
			v = "1923813";
			ds = DataSize.parse(v);
			System.out.println(ds);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
