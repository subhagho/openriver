/**
 * TODO: <comments>
 *
 * @file AdHocTests.java
 * @author subho
 * @date 24-Nov-2015
 */
package com.wookler.server.common;


import com.wookler.server.common.utils.PropertySubstitute;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 24-Nov-2015
 */
public class AdHocTests {
	public static final class TestR {
		private int		newv	= 12938920;
		private String	string	= "TEST-STRING";
		private boolean	test	= false;

		/**
		 * @return the newv
		 */
		public int getNewv() {
			return newv;
		}

		/**
		 * @param newv
		 *            the newv to set
		 */
		public void setNewv(int newv) {
			this.newv = newv;
		}

		/**
		 * @return the string
		 */
		public String getString() {
			return string;
		}

		/**
		 * @param string
		 *            the string to set
		 */
		public void setString(String string) {
			this.string = string;
		}

		/**
		 * @return the test
		 */
		public boolean isTest() {
			return test;
		}

		/**
		 * @param test
		 *            the test to set
		 */
		public void setTest(boolean test) {
			this.test = test;
		}
	}

	/**
	 * TODO: <comment>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String source = "This is a ${newv} ${string} for ${test}";
			TestR tr = new TestR();
			String value = PropertySubstitute.substitute(source, tr, false);
			System.out.println("RETURN : [" + value + "]");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
