/**
 * TODO: <comments>
 *
 * @file Test_ReflectionUtils.java
 * @author subho
 * @date 16-Nov-2015
 */
package com.wookler.server.common;

import java.lang.reflect.Field;

import com.wookler.server.common.utils.ReflectionUtils;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 16-Nov-2015
 */
public class Test_ReflectionUtils {
	public static enum ETest {
		E1, E2, E3
	}

	public static final class Input {
		private String	sv;
		private long	lv;
		private Long	llv;
		private boolean	bv;
		private Boolean	bbv;
		private ETest	ev;

		/**
		 * @return the sv
		 */
		public String getSv() {
			return sv;
		}

		/**
		 * @param sv
		 *            the sv to set
		 */
		public void setSv(String sv) {
			this.sv = sv;
		}

		/**
		 * @return the lv
		 */
		public long getLv() {
			return lv;
		}

		/**
		 * @param lv
		 *            the lv to set
		 */
		public void setLv(long lv) {
			this.lv = lv;
		}

		/**
		 * @return the llv
		 */
		public Long getLlv() {
			return llv;
		}

		/**
		 * @param llv
		 *            the llv to set
		 */
		public void setLlv(Long llv) {
			this.llv = llv;
		}

		/**
		 * @return the bv
		 */
		public boolean isBv() {
			return bv;
		}

		/**
		 * @param bv
		 *            the bv to set
		 */
		public void setBv(boolean bv) {
			this.bv = bv;
		}

		/**
		 * @return the bbv
		 */
		public Boolean getBbv() {
			return bbv;
		}

		/**
		 * @param bbv
		 *            the bbv to set
		 */
		public void setBbv(Boolean bbv) {
			this.bbv = bbv;
		}

		/**
		 * @return the ev
		 */
		public ETest getEv() {
			return ev;
		}

		/**
		 * @param ev
		 *            the ev to set
		 */
		public void setEv(ETest ev) {
			this.ev = ev;
		}
	}

	/**
	 * TODO: <comment>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Input i = new Input();
			Field[] fields = ReflectionUtils.getAllFields(i.getClass());
			for (Field f : fields) {
				if (ReflectionUtils.isPrimitiveTypeOrClass(f))
					System.out.println("Field=" + f.getName() + ", Type="
							+ f.getType() + ", Primitive="
							+ f.getType().isPrimitive());
				if (f.getName().equalsIgnoreCase("sv")) {
					ReflectionUtils.setValueFromString("Test String", i, f);
				} else if (f.getName().equalsIgnoreCase("lv")) {
					ReflectionUtils.setValueFromString(
							String.valueOf(System.currentTimeMillis()), i, f);
				} else if (f.getName().equalsIgnoreCase("llv")) {
					ReflectionUtils.setValueFromString(
							String.valueOf(System.currentTimeMillis()), i, f);
				} else if (f.getName().equalsIgnoreCase("bv")) {
					ReflectionUtils.setValueFromString("true", i, f);
				} else if (f.getName().equalsIgnoreCase("bbv")) {
					ReflectionUtils.setValueFromString("false", i, f);
				} else if (f.getName().equalsIgnoreCase("ev")) {
					ReflectionUtils.setValueFromString(ETest.E2.name(), i, f);
				}
			}

			for (Field f : fields) {
				String s = ReflectionUtils.strinfigy(i, f);
				System.out.println("Field:" + f.getName() + ", Value:" + s);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

}
