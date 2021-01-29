/*
 * Copyright (c) 2007 David Crawshaw <david@zentus.com>
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package org.sqlite.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;

import org.sqlite.BusyHandler;
import org.sqlite.Function;
import org.sqlite.ProgressHandler;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteJDBCLoader;

/** This class provides a thin JNI layer over the SQLite3 C API. */
public final class NativeDB extends DB
{
	public static enum SQLITEJDBC_STRING_CODING {
		ARRAY(1), BUFFER(2), STRING_CUTF8(3), STRING_JUTF8(4), STRING_CESU8(5);
		int value;
		SQLITEJDBC_STRING_CODING(int m) {
			value = m;
		}
	}
	
    /** SQLite connection handle. */
    long                   pointer       = 0;

    private static boolean isLoaded;
    private static boolean loadSucceeded;
    public static final SQLITEJDBC_STRING_CODING stringEncoding;
    
    
	/**
	 * try to speed up the performance in coding 
	 */
	private static final ThreadLocal<byte[]> byteBuffers;
    private static final ThreadLocal<char[]> charBuffers;
    private static final boolean default_utf8;
    

    static {
        if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
            System.loadLibrary("sqlitejdbc");
            isLoaded = true;
            loadSucceeded = true;
        } else {
            // continue with non Android execution path
            isLoaded = false;
            loadSucceeded = false;
        }
        String sqliteMode = System.getProperty("sqlitejdbc.string_coding");
        if (sqliteMode != null) {
       		stringEncoding = SQLITEJDBC_STRING_CODING.valueOf(sqliteMode);
        } else {
        	stringEncoding = SQLITEJDBC_STRING_CODING.ARRAY;
        }
        default_utf8 = Boolean.valueOf(System.getProperty("sqlitejdbc.default_utf8", "true"));
        int sqliteBuffer = Integer.getInteger("sqlitejdbc.buffer_size", 1 << 15);
        byteBuffers = ThreadLocal.withInitial(() -> new byte[sqliteBuffer]);
        charBuffers = ThreadLocal.withInitial(() -> new char[sqliteBuffer]);
    }

    public NativeDB(String url, String fileName, SQLiteConfig config)
            throws SQLException
    {
        super(url, fileName, config);
    }

    /**
     * Loads the SQLite interface backend.
     * @return True if the SQLite JDBC driver is successfully loaded; false otherwise.
     */
    public synchronized static boolean load() throws Exception {
        if (isLoaded)
            return loadSucceeded == true;

        loadSucceeded = SQLiteJDBCLoader.initialize();
        isLoaded = true;
        return loadSucceeded;
    }

    NativeDB checkDatabase() throws SQLException {
        if (pointer == 0) {
            throwex("The database has been closed");
        }
        return this;
    }

    long checkStatement(long stmt) throws SQLException {
        if (stmt == 0) {
            throwex("The prepared statement has been finalized");
        }
        return stmt;
    }

    /** linked list of all instanced UDFDatas */
    private final long udfdatalist = 0;

    // WRAPPER FUNCTIONS ////////////////////////////////////////////

    /**
     * @see org.sqlite.core.DB#_open(java.lang.String, int)
     */
    @Override
    protected synchronized void _open(String file, int openFlags) throws SQLException {
        if (pointer != 0) throwex("DB already open");
        this._open0(toObject(file), openFlags, stringEncoding.value);
    }

    native void _open0(Object file, int openFlags, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#_close()
     */
    @Override
    protected synchronized void _close() throws SQLException {
        if (pointer != 0) {
            this._close0();
        }
    }

    native void _close0() throws SQLException;

    /**
     * @see org.sqlite.core.DB#_exec(java.lang.String)
     */
    @Override
    public synchronized int _exec(String sql) throws SQLException {
        return checkDatabase()._exec0(toObject(sql), stringEncoding.value);
    }

    native int _exec0(Object sql, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#shared_cache(boolean)
     */
    @Override
    public synchronized int shared_cache(boolean enable) throws SQLException {
        return checkDatabase().shared_cache0(enable);
    }

    native int shared_cache0(boolean enable);

    /**
     * @see org.sqlite.core.DB#enable_load_extension(boolean)
     */
    @Override
    public synchronized int enable_load_extension(boolean enable) throws SQLException {
        return checkDatabase().enable_load_extension0(enable);
    }

    native int enable_load_extension0(boolean enable);

    /**
     * @see org.sqlite.core.DB#interrupt()
     */
    @Override
    public synchronized void interrupt() throws SQLException {
        checkDatabase().interrupt0();
    }

    native void interrupt0();

    /**
     * @see org.sqlite.core.DB#busy_timeout(int)
     */
    @Override
    public synchronized void busy_timeout(int ms) throws SQLException {
        checkDatabase().busy_timeout0(ms);
    }

    native void busy_timeout0(int ms);
    
    /**
     * @see org.sqlite.core.DB#busy_handler(BusyHandler)
     */
    @Override
    public synchronized void busy_handler(BusyHandler busyHandler) throws SQLException {
        checkDatabase().busy_handler0(busyHandler);
    }

    native void busy_handler0(BusyHandler busyHandler);

    /**
     * @see org.sqlite.core.DB#prepare(java.lang.String)
     */
    @Override
    protected synchronized long prepare(String sql) throws SQLException {
        return checkDatabase().prepare0(toObject(sql), stringEncoding.value);
    }

    native long prepare0(Object sql, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#errmsg()
     */
    @Override
    synchronized String errmsg() throws SQLException {
        return toString(checkDatabase().errmsg0(stringEncoding.value));
    }

    native Object errmsg0(int mode);

    private static String version;
    /**
     * @see org.sqlite.core.DB#libversion()
     */
    @Override
    public String libversion() {
        if (version == null) {
            version = toString(libversion0(stringEncoding.value));
        }
        return version;
    }

    native Object libversion0(int mode);

    /**
     * @see org.sqlite.core.DB#changes()
     */
    @Override
    public synchronized int changes() throws SQLException {
        return checkDatabase().changes0();
    }

    native int changes0();

    /**
     * @see org.sqlite.core.DB#total_changes()
     */
    @Override
    public synchronized int total_changes() throws SQLException {
        return checkDatabase().total_changes0();
    }

    native int total_changes0();

    /**
     * @see org.sqlite.core.DB#finalize(long)
     */
    @Override
    protected synchronized int finalize(long stmt) throws SQLException {
        return checkDatabase().finalize0(checkStatement(stmt));
    }

    native int finalize0(long stmt);

    /**
     * @see org.sqlite.core.DB#step(long)
     */
    @Override
    public synchronized int step(long stmt) throws SQLException {
        return checkDatabase().step0(checkStatement(stmt));
    }

    native int step0(long stmt);

    /**
     * @see org.sqlite.core.DB#reset(long)
     */
    @Override
    public synchronized int reset(long stmt) throws SQLException {
        return checkDatabase().reset0(checkStatement(stmt));
    }

    native int reset0(long stmt);

    /**
     * @see org.sqlite.core.DB#clear_bindings(long)
     */
    @Override
    public synchronized int clear_bindings(long stmt) throws SQLException {
        return checkDatabase().clear_bindings0(checkStatement(stmt));
    }

    native int clear_bindings0(long stmt);

    /**
     * @see org.sqlite.core.DB#bind_parameter_count(long)
     */
    @Override
    synchronized int bind_parameter_count(long stmt) throws SQLException {
        return checkDatabase().bind_parameter_count0(checkStatement(stmt));
    }

    native int bind_parameter_count0(long stmt);

    /**
     * @see org.sqlite.core.DB#column_count(long)
     */
    @Override
    public synchronized int column_count(long stmt) throws SQLException {
        return checkDatabase().column_count0(checkStatement(stmt));
    }

    native int column_count0(long stmt);

    /**
     * @see org.sqlite.core.DB#column_type(long, int)
     */
    @Override
    public synchronized int column_type(long stmt, int col) throws SQLException {
        return checkDatabase().column_type0(checkStatement(stmt), col);
    }

    native int column_type0(long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_decltype(long, int)
     */
    @Override
    public synchronized String column_decltype(long stmt, int col) throws SQLException {
        return toString(checkDatabase().column_decltype0(checkStatement(stmt), col, stringEncoding.value));
    }

    native Object column_decltype0(long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_table_name(long, int)
     */
    @Override
    public synchronized String column_table_name(long stmt, int col) throws SQLException {
        return toString(checkDatabase().column_table_name0(checkStatement(stmt), col, stringEncoding.value));
    }

    native Object column_table_name0(long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_name(long, int)
     */
    @Override
    public synchronized String column_name(long stmt, int col) throws SQLException {
        return toString(checkDatabase().column_name0(checkStatement(stmt), col, stringEncoding.value));
    }

    native Object column_name0(long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_text(long, int)
     */
    @Override
    public synchronized String column_text(long stmt, int col) throws SQLException {
        return toString(checkDatabase().column_text0(checkStatement(stmt), col, stringEncoding.value));
    }

    native Object column_text0(long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_blob(long, int)
     */
    @Override
    public synchronized byte[] column_blob(long stmt, int col) throws SQLException {
        return checkDatabase().column_blob0(checkStatement(stmt), col);
    }

    native byte[] column_blob0(long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_double(long, int)
     */
    @Override
    public synchronized double column_double(long stmt, int col) throws SQLException {
        return checkDatabase().column_double0(checkStatement(stmt), col);
    }

    native double column_double0(long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_long(long, int)
     */
    @Override
    public synchronized long column_long(long stmt, int col) throws SQLException {
        return checkDatabase().column_long0(checkStatement(stmt), col);
    }

    native long column_long0(long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_int(long, int)
     */
    @Override
    public synchronized int column_int(long stmt, int col) throws SQLException {
        return checkDatabase().column_int0(checkStatement(stmt), col);
    }

    native int column_int0(long stmt, int col);

    /**
     * @see org.sqlite.core.DB#bind_null(long, int)
     */
    @Override
    synchronized int bind_null(long stmt, int pos) throws SQLException {
        return checkDatabase().bind_null0(checkStatement(stmt), pos);
    }

    native int bind_null0(long stmt, int pos);

    /**
     * @see org.sqlite.core.DB#bind_int(long, int, int)
     */
    @Override
    synchronized int bind_int(long stmt, int pos, int v) throws SQLException {
        return checkDatabase().bind_int0(checkStatement(stmt), pos, v);
    }
    
    native int bind_int0(long stmt, int pos, int v);

    /**
     * @see org.sqlite.core.DB#bind_long(long, int, long)
     */
    @Override
    synchronized int bind_long(long stmt, int pos, long v) throws SQLException {
        return checkDatabase().bind_long0(checkStatement(stmt), pos, v);
    }

    native int bind_long0(long stmt, int pos, long v);

    /**
     * @see org.sqlite.core.DB#bind_double(long, int, double)
     */
    @Override
    synchronized int bind_double(long stmt, int pos, double v) throws SQLException {
        return checkDatabase().bind_double0(checkStatement(stmt), pos, v);
    }

    native int bind_double0(long stmt, int pos, double v);

    /**
     * @see org.sqlite.core.DB#bind_text(long, int, java.lang.String)
     */
    @Override
    synchronized int bind_text(long stmt, int pos, String v) throws SQLException {
        return checkDatabase().bind_text0(checkStatement(stmt), pos, toObject(v), stringEncoding.value);
    }

    native int bind_text0(long stmt, int pos, Object v, int mode);

    /**
     * @see org.sqlite.core.DB#bind_blob(long, int, byte[])
     */
    @Override
    synchronized int bind_blob(long stmt, int pos, byte[] v) throws SQLException {
        return checkDatabase().bind_blob0(checkStatement(stmt), pos, v);
    }
    
    native int bind_blob0(long stmt, int pos, byte[] v);

    /**
     * @see org.sqlite.core.DB#result_null(long)
     */
    @Override
    public synchronized void result_null(long context) throws SQLException {
        checkDatabase().result_null0(context);
    }

    native void result_null0(long context);

    /**
     * @see org.sqlite.core.DB#result_text(long, java.lang.String)
     */
    @Override
    public synchronized void result_text(long context, String val) throws SQLException {
    	checkDatabase().result_text0(context, toObject(val), stringEncoding.value);
    }

    native void result_text0(long context, Object val, int mode);

    /**
     * @see org.sqlite.core.DB#result_blob(long, byte[])
     */
    @Override
    public synchronized void result_blob(long context, byte[] val) throws SQLException {
        checkDatabase().result_blob0(context, val);
    }
    
    native void result_blob0(long context, byte[] val);

    /**
     * @see org.sqlite.core.DB#result_double(long, double)
     */
    @Override
    public synchronized void result_double(long context, double val) throws SQLException {
        checkDatabase().result_double0(context, val);
    }

    native void result_double0(long context, double val);

    /**
     * @see org.sqlite.core.DB#result_long(long, long)
     */
    @Override
    public synchronized void result_long(long context, long val) throws SQLException {
        checkDatabase().result_long0(context, val);
    }

    native void result_long0(long context, long val);

    /**
     * @see org.sqlite.core.DB#result_int(long, int)
     */
    @Override
    public synchronized void result_int(long context, int val) throws SQLException {
        checkDatabase().result_int0(context, val);
    }

    native void result_int0(long context, int val);

    /**
     * @see org.sqlite.core.DB#result_error(long, java.lang.String)
     */
    @Override
    public synchronized void result_error(long context, String err) throws SQLException {
    	checkDatabase().result_error0(context, toObject(err), stringEncoding.value);
    }

    native void result_error0(long context, Object err, int mode);

    /**
     * @see org.sqlite.core.DB#value_text(org.sqlite.Function, int)
     */
    @Override
    public synchronized String value_text(Function f, int arg) throws SQLException {
        return toString(checkDatabase().value_text0(f, arg, stringEncoding.value));
    }

    native Object value_text0(Function f, int arg, int mode);

    /**
     * @see org.sqlite.core.DB#value_blob(org.sqlite.Function, int)
     */
    @Override
    public synchronized byte[] value_blob(Function f, int arg) throws SQLException {
        return checkDatabase().value_blob0(f, arg);
    }

    native byte[] value_blob0(Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_double(org.sqlite.Function, int)
     */
    @Override
    public synchronized double value_double(Function f, int arg) throws SQLException {
        return checkDatabase().value_double0(f, arg);
    }

    native double value_double0(Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_long(org.sqlite.Function, int)
     */
    @Override
    public synchronized long value_long(Function f, int arg) throws SQLException {
        return checkDatabase().value_long0(f, arg);
    }

    native long value_long0(Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_int(org.sqlite.Function, int)
     */
    @Override
    public synchronized int value_int(Function f, int arg) throws SQLException {
        return checkDatabase().value_int0(f, arg);
    }

    native int value_int0(Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_type(org.sqlite.Function, int)
     */
    @Override
    public synchronized int value_type(Function f, int arg) throws SQLException {
        return checkDatabase().value_type0(f, arg);
    }

    native int value_type0(Function f, int arg);

    /**
     * @see org.sqlite.core.DB#create_function(java.lang.String, org.sqlite.Function, int, int)
     */
    @Override
    public synchronized int create_function(String name, Function func, int nArgs, int flags) throws SQLException {
        return checkDatabase().create_function0(toObject(name), func, nArgs, flags, stringEncoding.value);
    }

    native int create_function0(Object name, Function func, int nArgs, int flags, int mode);

    /**
     * @see org.sqlite.core.DB#destroy_function(java.lang.String, int)
     */
    @Override
    public synchronized int destroy_function(String name, int nArgs) throws SQLException {
        return checkDatabase().destroy_function0(toObject(name), nArgs, stringEncoding.value);
    }

    native int destroy_function0(Object name, int nArgs, int mode);

    /**
     * @see org.sqlite.core.DB#free_functions()
     */
    @Override
    synchronized void free_functions() throws SQLException {
        checkDatabase().free_functions0();
    }

    native void free_functions0();

    @Override
    public synchronized int limit(int id, int value) throws SQLException {
        return checkDatabase().limit0(id, value);
    }

    native int limit0(int id, int value) throws SQLException;

    /**
     * @see org.sqlite.core.DB#backup(java.lang.String, java.lang.String, org.sqlite.core.DB.ProgressObserver)
     */
    @Override
    public synchronized int backup(String dbName, String destFileName, ProgressObserver observer) throws SQLException {
        return checkDatabase().backup0(toObject(dbName), toObject(destFileName), observer, stringEncoding.value);
    }
    
    native int backup0(Object dbName, Object destFileName, ProgressObserver observer, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#restore(java.lang.String, java.lang.String,
     *      org.sqlite.core.DB.ProgressObserver)
     */
    @Override
    public synchronized int restore(String dbName, String sourceFileName, ProgressObserver observer)
            throws SQLException {
        return checkDatabase().restore0(toObject(dbName), toObject(sourceFileName), observer, stringEncoding.value);
    }
    
    native int restore0(Object dbName, Object sourceFileName, ProgressObserver observer, int mode) throws SQLException;

    // COMPOUND FUNCTIONS (for optimisation) /////////////////////////
    String toString(Object object) {
    	if (object == null)
    		return null;
    	if (object instanceof String) {
    		return (String)object;
    	}
    	if (object instanceof ByteBuffer) {
    		ByteBuffer buf = (ByteBuffer) object;
    		int limit = buf.limit();
    		if (limit == 0) 
    			return "";
    		byte[] arr = byteBuffers.get();
    		if (limit > arr.length) {
    			arr = new byte[limit];
    		}
            buf.get(arr, 0, limit);
            if (default_utf8) {
                return new String(arr, 0, limit, StandardCharsets.UTF_8);
            }
            return UTF8ToUTF16(charBuffers.get(), arr, limit);
    	}
    	if (object instanceof byte[]) {
            byte[] arr = (byte[]) object;
            int limit = arr.length;
    		if (limit == 0)
                return "";
            if (default_utf8) {
                return new String(arr, 0, limit, StandardCharsets.UTF_8);
            }
            return UTF8ToUTF16(charBuffers.get(), arr, limit);
    	}
		return object.toString();
    }
    
    Object toObject(String string) {
    	if (string == null)
    		return null;
    	switch (stringEncoding) {
            case ARRAY:
            case BUFFER:
                if (default_utf8) {
                    return string.getBytes(StandardCharsets.UTF_8);
                }
                return UTF16ToUTF8(byteBuffers.get(), string);
            case STRING_CUTF8:
            case STRING_JUTF8:
            case STRING_CESU8:
            default:
    		    return string;
		}
    }

	byte[] UTF16ToUTF8(byte[] buf, String src) {
        int size = src.length(), 
            sp = 0, limit = size * 4;
		byte[] dst = limit < buf.length ? buf : new byte[limit];
		for (int i = 0; i < size; ) {
            char w1 = src.charAt(i++);
			if (w1 < 0x80) {
                dst[sp++] = (byte)w1;
			} else if (w1 < 0x800) {
                dst[sp++] = (byte)(((w1 >> 6) & 0x1F) ^ 0xC0);
                dst[sp++] = (byte)((w1 & 0x3F) ^ 0x80);
            } else if ((w1 < 0xD800) || (w1 > 0xDFFF)) {
                dst[sp++] = (byte)(((w1 >>12) & 0x0F) ^ 0xE0);
                dst[sp++] = (byte)(((w1 >> 6) & 0x3F) ^ 0x80);
                dst[sp++] = (byte)((w1 & 0x3F) ^ 0x80);
            } else if (w1 < 0xDC00) {
                if (i == size) return null;
                char w2 = src.charAt(i++);
                // if (w2 < 0xDC00 || w2 > 0xDFFF) return null;
                int uc = (((w1 & 0x3FF) << 10) ^ (w2 & 0x3FF)) + 0x10000;
                dst[sp++] = (byte)(((uc >>18) & 0x07) ^ 0xF0);
                dst[sp++] = (byte)(((uc >>12) & 0x3F) ^ 0x80);
                dst[sp++] = (byte)(((uc >> 6) & 0x3F) ^ 0x80);
                dst[sp++] = (byte)((uc & 0x3F) ^ 0x80);
            } else {
                return null;
            }
		}    
		return sp != dst.length 
			? Arrays.copyOf(dst, sp)
			: dst;
	}

	String UTF8ToUTF16(char[] buf, byte[] src, int size) {
        int sp = 0, limit = size * 2;
		char[] dst = limit < buf.length ? buf : new char[limit];
		for (int i = 0; i < size; ) {
            char w1 = (char)(src[i++] & 0xFF);
			if (w1 < 0x80) {
                dst[sp++] = (char)w1;
			} else if (w1 < 0xE0) {
                if (w1 < 0xC0 || i == size)  return null;
                char w2 = (char)(src[i++] & 0xFF);
                // if ((w2 & 0xC0) != 0x80) return null;
                dst[sp++] = (char)(((w1 & 0x1F) << 6) ^ (w2 & 0x3F));
			} else if (w1 < 0xF0) {
                if (i + 1 == size)  return null;
                char w2 = (char)(src[i++] & 0xFF);
                char w3 = (char)(src[i++] & 0xFF);
                // if ((w2 & 0xC0) != 0x80 || (w3 & 0xC0) != 0x80) return null;
                dst[sp++] = (char)(((w1 & 0x0F) << 12) ^ ((w2 & 0x3F) << 6) ^ (w3 & 0x3F));
			} else if (w1 < 0xF8) {
                if (i + 2 == size)  return null;
                char w2 = (char)(src[i++] & 0xFF);
                char w3 = (char)(src[i++] & 0xFF);
                char w4 = (char)(src[i++] & 0xFF);
                // if ((w2 & 0xC0) != 0x80 || (w3 & 0xC0) != 0x80 || (w4 & 0xC0) != 0x80) return null;
                int uc = (((w1 & 0x07) << 18) ^ ((w2 & 0x3F) << 12) ^ ((w3 & 0x3F) << 6) ^ (w4 & 0x3F)) - 0x10000;
                dst[sp++] = (char)(((uc >> 10) & 0x3FF) ^ 0xD800);
                dst[sp++] = (char)((uc & 0x3FF) ^ 0xDC00);
			} else {
                return null;
			}
		}
		
		return new String(dst, 0, sp);
	}
    /**
     * Provides metadata for table columns.
     * @returns For each column returns: <br/>
     * res[col][0] = true if column constrained NOT NULL<br/>
     * res[col][1] = true if column is part of the primary key<br/>
     * res[col][2] = true if column is auto-increment.
     * @see org.sqlite.core.DB#column_metadata(long)
     */
    @Override
    synchronized boolean[][] column_metadata(long stmt) throws SQLException {
        return checkDatabase().column_metadata0(checkStatement(stmt));
    }

    native boolean[][] column_metadata0(long stmt);

    @Override
    synchronized void set_commit_listener(boolean enabled) throws SQLException {
        checkDatabase().set_commit_listener0(enabled);
    }

    native void set_commit_listener0(boolean enabled);

    @Override
    synchronized void set_update_listener(boolean enabled) throws SQLException {
        checkDatabase().set_update_listener0(enabled);
    }

    native void set_update_listener0(boolean enabled);

    /**
     * Throws an SQLException
     * @param msg Message for the SQLException.
     * @throws SQLException
     */
    static void throwex(String msg) throws SQLException {
        throw new SQLException(msg);
    }
    
    public synchronized void register_progress_handler(int vmCalls, ProgressHandler progressHandler) throws SQLException {
        checkDatabase().register_progress_handler0(vmCalls, progressHandler);
    }

    native void register_progress_handler0(int vmCalls, ProgressHandler progressHandler);

    public synchronized void clear_progress_handler() throws SQLException {
        checkDatabase().clear_progress_handler0();
    }

    native void clear_progress_handler0();
}