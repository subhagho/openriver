package com.wookler.server.common.structs;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by subghosh on 3/10/15.
 */
public class ACL {
    private static final class Constants {
        public static final short OFFSET_OWNER = 6;
        public static final short OFFSET_GROUP = 3;
        public static final short OFFSET_OTHERS = 0;
        public static final short OFFSET_READ = 2;
        public static final short OFFSET_WRITE = 1;
        @SuppressWarnings("unused")
		public static final short OFFSET_EXEC = 0;
    }

    private int bitmap = 0;

    /**
	 * @return the bitmap
	 */
	public int getBitmap() {
		return bitmap;
	}

	/**
	 * @param bitmap the bitmap to set
	 */
	public void setBitmap(int bitmap) {
		this.bitmap = bitmap;
	}

	public ACL set(short bit) {
        Preconditions.checkArgument(bit >= 0 && bit < 16);
        bitmap = (short) (bitmap | (1 << bit));
        return this;
    }

    public ACL unset(short bit) {
        Preconditions.checkArgument(bit >= 0 && bit < 16);
        bitmap = (short) (bitmap & ~(1 << bit));
        return this;
    }

    public ACL set(String value) {
        Preconditions.checkArgument(value != null && !StringUtils.isEmpty(value) && value
                .length() == 3);
        char c = value.charAt(0);

        if (c == 'u') {
            set(Constants.OFFSET_OWNER, value);
        } else if (c == 'g') {
            set(Constants.OFFSET_GROUP, value);
        } else if (c == 'o') {
            set(Constants.OFFSET_OTHERS, value);
        }
        return this;
    }

    public boolean check(String op) {
        Preconditions.checkArgument(!StringUtils.isEmpty(op) && op.length() == 2);
        char t = op.charAt(0);
        char o = op.charAt(1);

        short offset = Constants.OFFSET_OTHERS;
        if (t == 'u') {
            offset = Constants.OFFSET_OWNER;
        } else if (t == 'g') {
            offset = Constants.OFFSET_GROUP;
        } else if (t != 'o') {
            return false;
        }
        if (o == 'r') {
            offset += Constants.OFFSET_READ;
        } else if (o == 'w') {
            offset += Constants.OFFSET_WRITE;
        } else if (o != 'x') {
            return false;
        }

        return get(offset++);
    }

    private boolean get(short offset) {
        return (bitmap & (1L << offset)) != 0;
    }

    private void set(short offset, String bits) {
        char s = bits.charAt(1);
        char o = bits.charAt(2);
        if (o == 'w') {
            offset += Constants.OFFSET_WRITE;
        } else if (o == 'r') {
            offset += Constants.OFFSET_READ;
        } else if (o != 'x') {
            return;
        }
        if (s == '+') {
            set(offset);
        } else if (s == '-') {
            unset(offset);
        }
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p/>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return Long.toBinaryString(bitmap);
    }
}
