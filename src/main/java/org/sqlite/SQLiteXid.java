package org.sqlite;
import java.util.StringTokenizer;
import javax.transaction.xa.Xid;

//******************************************************************************
//**  SQLiteXid
//******************************************************************************
/**
 *   An object of this class represents a transaction id.
 *
 *   Port from H2 JdbcXid:
 *   http://code.google.com/p/h2database/source/browse/trunk/h2/src/main/org/h2/jdbcx/JdbcXid.java
 *
 ******************************************************************************/

public class SQLiteXid implements Xid {

    private static final String PREFIX = "XID";

    private int formatId;
    private byte[] branchQualifier;
    private byte[] globalTransactionId;

    SQLiteXid(SQLiteDataSourceFactory factory, int id, String tid) {
        //setTrace(factory.getTrace(), TraceObject.XID, id);
        try {
            StringTokenizer tokenizer = new StringTokenizer(tid, "_");
            String prefix = tokenizer.nextToken();
            if (!PREFIX.equals(prefix)) {
                //throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
            }
            formatId = Integer.parseInt(tokenizer.nextToken());
            branchQualifier = convertHexToBytes(tokenizer.nextToken());
            globalTransactionId = convertHexToBytes(tokenizer.nextToken());
        } catch (RuntimeException e) {
            //throw DbException.get(ErrorCode.WRONG_XID_FORMAT_1, tid);
        }
    }

    /**
     * INTERNAL
     */
    public static String toString(Xid xid) {
        StringBuilder buff = new StringBuilder(PREFIX);
        buff.append('_').
            append(xid.getFormatId()).
            append('_').
            append(convertBytesToHex(xid.getBranchQualifier())).
            append('_').
            append(convertBytesToHex(xid.getGlobalTransactionId()));
        return buff.toString();
    }

    /**
     * Get the format id.
     *
     * @return the format id
     */
    public int getFormatId() {
        //debugCodeCall("getFormatId");
        return formatId;
    }

    /**
     * The transaction branch identifier.
     *
     * @return the identifier
     */
    public byte[] getBranchQualifier() {
        //debugCodeCall("getBranchQualifier");
        return branchQualifier;
    }

    /**
     * The global transaction identifier.
     *
     * @return the transaction id
     */
    public byte[] getGlobalTransactionId() {
        //debugCodeCall("getGlobalTransactionId");
        return globalTransactionId;
    }





    /*
     The following methods were ported from org.h2.util.StringUtils.java

     */

    private static final char[] HEX = "0123456789abcdef".toCharArray();



    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param value the byte array
     * @return the hex encoded string
     */
    private static String convertBytesToHex(byte[] value) {
        return convertBytesToHex(value, value.length);
    }

    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param value the byte array
     * @param len the number of bytes to encode
     * @return the hex encoded string
     */
    private static String convertBytesToHex(byte[] value, int len) {
        char[] buff = new char[len + len];
        char[] hex = HEX;
        for (int i = 0; i < len; i++) {
            int c = value[i] & 0xff;
            buff[i + i] = hex[c >> 4];
            buff[i + i + 1] = hex[c & 0xf];
        }
        return new String(buff);
    }
    /**
     * Convert a hex encoded string to a byte array.
     *
     * @param s the hex encoded string
     * @return the byte array
     */
    private static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            //throw DbException.get(ErrorCode.HEX_STRING_ODD_1, s);
        }
        len /= 2;
        byte[] buff = new byte[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (byte) ((getHexDigit(s, i + i) << 4) | getHexDigit(s, i + i + 1));
        }
        return buff;
    }

    private static int getHexDigit(String s, int i) {
        char c = s.charAt(i);
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return c - 'a' + 0xa;
        } else if (c >= 'A' && c <= 'F') {
            return c - 'A' + 0xa;
        } else {
            return -99999;
            //throw DbException.get(ErrorCode.HEX_STRING_WRONG_1, s);
        }
    }

}