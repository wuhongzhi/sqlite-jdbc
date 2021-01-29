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

    NativeDB checkDb() throws SQLException {
        if (pointer == 0) {
            throwex("The database has been closed");
        }
        return this;
    }

    static long checkStatement(long stmt) throws SQLException {
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
        _open0(this, toObject(file), openFlags, stringEncoding.value);
    }

    static native void _open0(NativeDB db, Object file, int openFlags, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#_close()
     */
    @Override
    protected synchronized void _close() throws SQLException {
        if (pointer != 0) {
            _close0(this);
        }
    }

    static native void _close0(NativeDB db) throws SQLException;

    /**
     * @see org.sqlite.core.DB#_exec(java.lang.String)
     */
    @Override
    public synchronized int _exec(String sql) throws SQLException {
        return _exec0(checkDb(), toObject(sql), stringEncoding.value);
    }

    static native int _exec0(NativeDB db, Object sql, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#shared_cache(boolean)
     */
    @Override
    public synchronized int shared_cache(boolean enable) throws SQLException {
        return shared_cache0(checkDb(), enable);
    }

    static native int shared_cache0(NativeDB db, boolean enable);

    /**
     * @see org.sqlite.core.DB#enable_load_extension(boolean)
     */
    @Override
    public synchronized int enable_load_extension(boolean enable) throws SQLException {
        return enable_load_extension0(checkDb(), enable);
    }

    static native int enable_load_extension0(NativeDB db, boolean enable);

    /**
     * @see org.sqlite.core.DB#interrupt()
     */
    @Override
    public synchronized void interrupt() throws SQLException {
        interrupt0(checkDb());
    }

    static native void interrupt0(NativeDB db);

    /**
     * @see org.sqlite.core.DB#busy_timeout(int)
     */
    @Override
    public synchronized void busy_timeout(int ms) throws SQLException {
        busy_timeout0(checkDb(), ms);
    }

    static native void busy_timeout0(NativeDB db, int ms);
    
    /**
     * @see org.sqlite.core.DB#busy_handler(BusyHandler)
     */
    @Override
    public synchronized void busy_handler(BusyHandler busyHandler) throws SQLException {
        busy_handler0(checkDb(), busyHandler);
    }

    static native void busy_handler0(NativeDB db, BusyHandler busyHandler);

    /**
     * @see org.sqlite.core.DB#prepare(java.lang.String)
     */
    @Override
    protected synchronized long prepare(String sql) throws SQLException {
        return prepare0(checkDb(), toObject(sql), stringEncoding.value);
    }

    static native long prepare0(NativeDB db, Object sql, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#errmsg()
     */
    @Override
    synchronized String errmsg() throws SQLException {
        return toString(errmsg0(checkDb(), stringEncoding.value));
    }

    static native Object errmsg0(NativeDB db, int mode);

    /**
     * @see org.sqlite.core.DB#libversion()
     */
    @Override
    public String libversion() {
        return toString(libversion0(stringEncoding.value));
    }

    static native Object libversion0(int mode);

    /**
     * @see org.sqlite.core.DB#changes()
     */
    @Override
    public synchronized int changes() throws SQLException {
        return changes0(checkDb());
    }

    static native int changes0(NativeDB db);

    /**
     * @see org.sqlite.core.DB#total_changes()
     */
    @Override
    public synchronized int total_changes() throws SQLException {
        return total_changes0(checkDb());
    }

    static native int total_changes0(NativeDB db);

    /**
     * @see org.sqlite.core.DB#finalize(long)
     */
    @Override
    protected synchronized int finalize(long stmt) throws SQLException {
        return finalize0(checkDb(), checkStatement(stmt));
    }

    static native int finalize0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#step(long)
     */
    @Override
    public synchronized int step(long stmt) throws SQLException {
        return step0(checkDb(), checkStatement(stmt));
    }

    static native int step0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#reset(long)
     */
    @Override
    public synchronized int reset(long stmt) throws SQLException {
        return reset0(checkDb(), checkStatement(stmt));
    }

    static native int reset0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#clear_bindings(long)
     */
    @Override
    public synchronized int clear_bindings(long stmt) throws SQLException {
        return clear_bindings0(checkDb(), checkStatement(stmt));
    }

    static native int clear_bindings0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#bind_parameter_count(long)
     */
    @Override
    synchronized int bind_parameter_count(long stmt) throws SQLException {
        return bind_parameter_count0(checkDb(), checkStatement(stmt));
    }

    static native int bind_parameter_count0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#column_count(long)
     */
    @Override
    public synchronized int column_count(long stmt) throws SQLException {
        return column_count0(checkDb(), checkStatement(stmt));
    }

    static native int column_count0(NativeDB db, long stmt);

    /**
     * @see org.sqlite.core.DB#column_type(long, int)
     */
    @Override
    public synchronized int column_type(long stmt, int col) throws SQLException {
        return column_type0(checkDb(), checkStatement(stmt), col);
    }

    static native int column_type0(NativeDB db, long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_decltype(long, int)
     */
    @Override
    public synchronized String column_decltype(long stmt, int col) throws SQLException {
        return toString(column_decltype0(checkDb(), checkStatement(stmt), col, stringEncoding.value));
    }

    static native Object column_decltype0(NativeDB db, long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_table_name(long, int)
     */
    @Override
    public synchronized String column_table_name(long stmt, int col) throws SQLException {
        return toString(column_table_name0(checkDb(), checkStatement(stmt), col, stringEncoding.value));
    }

    static native Object column_table_name0(NativeDB db, long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_name(long, int)
     */
    @Override
    public synchronized String column_name(long stmt, int col) throws SQLException {
        return toString(column_name0(checkDb(), checkStatement(stmt), col, stringEncoding.value));
    }

    static native Object column_name0(NativeDB db, long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_text(long, int)
     */
    @Override
    public synchronized String column_text(long stmt, int col) throws SQLException {
        return toString(column_text0(checkDb(), checkStatement(stmt), col, stringEncoding.value));
    }

    static native Object column_text0(NativeDB db, long stmt, int col, int mode);

    /**
     * @see org.sqlite.core.DB#column_blob(long, int)
     */
    @Override
    public synchronized byte[] column_blob(long stmt, int col) throws SQLException {
        return column_blob0(checkDb(), checkStatement(stmt), col);
    }

    static native byte[] column_blob0(NativeDB db, long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_double(long, int)
     */
    @Override
    public synchronized double column_double(long stmt, int col) throws SQLException {
        return column_double0(checkDb(), checkStatement(stmt), col);
    }

    static native double column_double0(NativeDB db, long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_long(long, int)
     */
    @Override
    public synchronized long column_long(long stmt, int col) throws SQLException {
        return column_long0(checkDb(), checkStatement(stmt), col);
    }

    static native long column_long0(NativeDB db, long stmt, int col);

    /**
     * @see org.sqlite.core.DB#column_int(long, int)
     */
    @Override
    public synchronized int column_int(long stmt, int col) throws SQLException {
        return column_int0(checkDb(), checkStatement(stmt), col);
    }

    static native int column_int0(NativeDB db, long stmt, int col);

    /**
     * @see org.sqlite.core.DB#bind_null(long, int)
     */
    @Override
    synchronized int bind_null(long stmt, int pos) throws SQLException {
        return bind_null0(checkDb(), checkStatement(stmt), pos);
    }

    static native int bind_null0(NativeDB db, long stmt, int pos);

    /**
     * @see org.sqlite.core.DB#bind_int(long, int, int)
     */
    @Override
    synchronized int bind_int(long stmt, int pos, int v) throws SQLException {
        return bind_int0(checkDb(), checkStatement(stmt), pos, v);
    }
    
    static native int bind_int0(NativeDB db, long stmt, int pos, int v);

    /**
     * @see org.sqlite.core.DB#bind_long(long, int, long)
     */
    @Override
    synchronized int bind_long(long stmt, int pos, long v) throws SQLException {
        return bind_long0(checkDb(), checkStatement(stmt), pos, v);
    }

    static native int bind_long0(NativeDB db, long stmt, int pos, long v);

    /**
     * @see org.sqlite.core.DB#bind_double(long, int, double)
     */
    @Override
    synchronized int bind_double(long stmt, int pos, double v) throws SQLException {
        return bind_double0(checkDb(), checkStatement(stmt), pos, v);
    }

    static native int bind_double0(NativeDB db, long stmt, int pos, double v);

    /**
     * @see org.sqlite.core.DB#bind_text(long, int, java.lang.String)
     */
    @Override
    synchronized int bind_text(long stmt, int pos, String v) throws SQLException {
        return bind_text0(checkDb(), checkStatement(stmt), pos, toObject(v), stringEncoding.value);
    }

    static native int bind_text0(NativeDB db, long stmt, int pos, Object v, int mode);

    /**
     * @see org.sqlite.core.DB#bind_blob(long, int, byte[])
     */
    @Override
    synchronized int bind_blob(long stmt, int pos, byte[] v) throws SQLException {
        return bind_blob0(checkDb(), checkStatement(stmt), pos, v);
    }
    
    static native int bind_blob0(NativeDB db, long stmt, int pos, byte[] v);

    /**
     * @see org.sqlite.core.DB#result_null(long)
     */
    @Override
    public synchronized void result_null(long context) throws SQLException {
        result_null0(checkDb(), context);
    }

    static native void result_null0(NativeDB db, long context);

    /**
     * @see org.sqlite.core.DB#result_text(long, java.lang.String)
     */
    @Override
    public synchronized void result_text(long context, String val) throws SQLException {
    	result_text0(checkDb(), context, toObject(val), stringEncoding.value);
    }

    static native void result_text0(NativeDB db, long context, Object val, int mode);

    /**
     * @see org.sqlite.core.DB#result_blob(long, byte[])
     */
    @Override
    public synchronized void result_blob(long context, byte[] val) throws SQLException {
        result_blob0(checkDb(), context, val);
    }
    
    static native void result_blob0(NativeDB db, long context, byte[] val);

    /**
     * @see org.sqlite.core.DB#result_double(long, double)
     */
    @Override
    public synchronized void result_double(long context, double val) throws SQLException {
        result_double0(checkDb(), context, val);
    }

    static native void result_double0(NativeDB db, long context, double val);

    /**
     * @see org.sqlite.core.DB#result_long(long, long)
     */
    @Override
    public synchronized void result_long(long context, long val) throws SQLException {
        result_long0(checkDb(), context, val);
    }

    static native void result_long0(NativeDB db, long context, long val);

    /**
     * @see org.sqlite.core.DB#result_int(long, int)
     */
    @Override
    public synchronized void result_int(long context, int val) throws SQLException {
        result_int0(checkDb(), context, val);

    }

    static native void result_int0(NativeDB db, long context, int val);

    /**
     * @see org.sqlite.core.DB#result_error(long, java.lang.String)
     */
    @Override
    public synchronized void result_error(long context, String err) throws SQLException {
    	result_error0(checkDb(), context, toObject(err), stringEncoding.value);
    }

    static native void result_error0(NativeDB db, long context, Object err, int mode);

    /**
     * @see org.sqlite.core.DB#value_text(org.sqlite.Function, int)
     */
    @Override
    public synchronized String value_text(Function f, int arg) throws SQLException {
        return toString(value_text0(checkDb(), f, arg, stringEncoding.value));
    }

    static native Object value_text0(NativeDB db, Function f, int arg, int mode);

    /**
     * @see org.sqlite.core.DB#value_blob(org.sqlite.Function, int)
     */
    @Override
    public synchronized byte[] value_blob(Function f, int arg) throws SQLException {
        return value_blob0(checkDb(), f, arg);
    }

    static native byte[] value_blob0(NativeDB db, Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_double(org.sqlite.Function, int)
     */
    @Override
    public synchronized double value_double(Function f, int arg) throws SQLException {
        return value_double0(checkDb(),f, arg);
    }

    static native double value_double0(NativeDB db, Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_long(org.sqlite.Function, int)
     */
    @Override
    public synchronized long value_long(Function f, int arg) throws SQLException {
        return value_long0(checkDb(), f, arg);
    }

    static native long value_long0(NativeDB db, Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_int(org.sqlite.Function, int)
     */
    @Override
    public synchronized int value_int(Function f, int arg) throws SQLException {
        return value_int0(checkDb(), f, arg);
    }

    static native int value_int0(NativeDB db, Function f, int arg);

    /**
     * @see org.sqlite.core.DB#value_type(org.sqlite.Function, int)
     */
    @Override
    public synchronized int value_type(Function f, int arg) throws SQLException {
        return value_type0(checkDb(), f, arg);
    }

    static native int value_type0(NativeDB db, Function f, int arg);

    /**
     * @see org.sqlite.core.DB#create_function(java.lang.String, org.sqlite.Function, int, int)
     */
    @Override
    public synchronized int create_function(String name, Function func, int nArgs, int flags) throws SQLException {
        return create_function0(checkDb(), toObject(name), func, nArgs, flags, stringEncoding.value);
    }

    static native int create_function0(NativeDB db, Object name, Function func, int nArgs, int flags, int mode);

    /**
     * @see org.sqlite.core.DB#destroy_function(java.lang.String, int)
     */
    @Override
    public synchronized int destroy_function(String name, int nArgs) throws SQLException {
        return destroy_function0(checkDb(), toObject(name), nArgs, stringEncoding.value);
    }

    static native int destroy_function0(NativeDB db, Object name, int nArgs, int mode);

    /**
     * @see org.sqlite.core.DB#free_functions()
     */
    @Override
    synchronized void free_functions() throws SQLException {
        free_functions0(checkDb());
    }

    static native void free_functions0(NativeDB db);

    @Override
    public synchronized int limit(int id, int value) throws SQLException {
        return limit0(checkDb(), id, value);
    }

    static native int limit0(NativeDB db, int id, int value) throws SQLException;

    /**
     * @see org.sqlite.core.DB#backup(java.lang.String, java.lang.String, org.sqlite.core.DB.ProgressObserver)
     */
    @Override
    public synchronized int backup(String dbName, String destFileName, ProgressObserver observer) throws SQLException {
        return backup0(checkDb(), toObject(dbName), toObject(destFileName), observer, stringEncoding.value);
    }
    
    static native int backup0(NativeDB db, Object dbName, Object destFileName, ProgressObserver observer, int mode) throws SQLException;

    /**
     * @see org.sqlite.core.DB#restore(java.lang.String, java.lang.String,
     *      org.sqlite.core.DB.ProgressObserver)
     */
    @Override
    public synchronized int restore(String dbName, String sourceFileName, ProgressObserver observer)
            throws SQLException {
        return restore0(checkDb(), toObject(dbName), toObject(sourceFileName), observer, stringEncoding.value);
    }
    
    static native int restore0(NativeDB db, Object dbName, Object sourceFileName, ProgressObserver observer, int mode) throws SQLException;

    // COMPOUND FUNCTIONS (for optimisation) /////////////////////////
    static String toString(Object object) {
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
    
    static Object toObject(String string) {
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

	static byte[] UTF16ToUTF8(byte[] buf, String src) {
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

	static String UTF8ToUTF16(char[] buf, byte[] src, int size) {
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
        return column_metadata0(checkDb(), checkStatement(stmt));
    }

    static native boolean[][] column_metadata0(NativeDB db, long stmt);

    @Override
    synchronized void set_commit_listener(boolean enabled) throws SQLException {
        set_commit_listener0(checkDb(), enabled);
    }

    static native void set_commit_listener0(NativeDB db, boolean enabled);

    @Override
    synchronized void set_update_listener(boolean enabled) throws SQLException {
        set_update_listener0(checkDb(), enabled);
    }

    static native void set_update_listener0(NativeDB db, boolean enabled);

    /**
     * Throws an SQLException
     * @param msg Message for the SQLException.
     * @throws SQLException
     */
    static void throwex(String msg) throws SQLException {
        throw new SQLException(msg);
    }
    
    public synchronized void register_progress_handler(int vmCalls, ProgressHandler progressHandler) throws SQLException {
        register_progress_handler0(checkDb(), vmCalls, progressHandler);
    }

    static native void register_progress_handler0(NativeDB db, int vmCalls, ProgressHandler progressHandler);

    public synchronized void clear_progress_handler() throws SQLException {
        clear_progress_handler0(checkDb());
    }

    static native void clear_progress_handler0(NativeDB db);
}