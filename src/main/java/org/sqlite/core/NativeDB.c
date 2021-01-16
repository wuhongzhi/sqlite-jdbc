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

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include "NativeDB.h"
#include "sqlite3.h"

static jclass dbclass = 0;
static jclass fclass = 0;
static jclass aclass = 0;
static jclass wclass = 0;
static jclass pclass = 0;
static jclass phandleclass = 0;

static jclass arrclass = 0;
static jclass bufclass = 0;
static jclass strclass = 0;

static jobject strencoding = 0;

typedef enum {
    ARRAY = 1, BUFFER, STRING_CUTF8, STRING_JUTF8, STRING_CESU8
} SQLITEJDBC_STRING_CODING;

inline static void * toref(jlong value)
{
    jvalue ret;
    ret.j = value;
    return (void *) ret.l;
}

inline static jlong fromref(void * value)
{
    jvalue ret;
    ret.l = value;
    return ret.j;
}

inline static void throwex(JNIEnv *env, jobject this)
{
    static jmethodID mth_throwex = 0;

    if (!mth_throwex)
        mth_throwex = (*env)->GetMethodID(env, dbclass, "throwex", "()V");

    (*env)->CallVoidMethod(env, this, mth_throwex);
}

inline static void throwex_errorcode(JNIEnv *env, jobject this, int errorCode)
{
    static jmethodID mth_throwex = 0;

    if (!mth_throwex)
        mth_throwex = (*env)->GetMethodID(env, dbclass, "throwex", "(I)V");

    (*env)->CallVoidMethod(env, this, mth_throwex, (jint) errorCode);
}

inline static void throwex_msg(JNIEnv *env, const char *str)
{
    static jmethodID mth_throwexmsg = 0;

    if (!mth_throwexmsg) mth_throwexmsg = (*env)->GetStaticMethodID(
            env, dbclass, "throwex", "(Ljava/lang/String;)V");

    jstring msg = (*env)->NewStringUTF(env, str);
    (*env)->CallStaticVoidMethod(env, dbclass, mth_throwexmsg, msg);
}

inline static void throwex_outofmemory(JNIEnv *env)
{
    throwex_msg(env, "Out of memory");
}

inline static void throwex_stmt_finalized(JNIEnv *env)
{
    throwex_msg(env, "The prepared statement has been finalized");
}

inline static void throwex_db_closed(JNIEnv *env)
{
    throwex_msg(env, "The database has been closed");
}

inline static jbyteArray bytesToArray(JNIEnv *env, const char* bytes, jsize length) {
    jbyteArray array;

    array = (*env)->NewByteArray(env, length);
    if (!array) {
        throwex_outofmemory(env);
        return NULL;
    }
    if (length) {
        (*env)->SetByteArrayRegion(env, array, 0, length, bytes);
    }
    return array;
}

inline static char* arrayToBytes(JNIEnv *env, const jarray array, jsize* size) {
    jsize length;
    char* ret = NULL;
    
    length = (*env)->GetArrayLength(env, array);
    ret = malloc(length + 1);    
    if (!ret) {
        throwex_outofmemory(env);
        return NULL;
    }
    
    char* src;
    src = (*env)->GetPrimitiveArrayCritical(env, array, JNI_FALSE);
    memcpy(ret, src, length);
    (*env)->ReleasePrimitiveArrayCritical(env, array, src, JNI_ABORT);

    ret[length] = '\0';
    if (size) {
        *size = length; 
    }
    return ret;
}

inline static jboolean UTF16toUTF8(JNIEnv *env, const jchar* src, char* dst, jsize size, jsize* out) {
    if (out) {
        *out = 0;
    }

    jint sp = 0, i;
    for (i = 0; i < size; i++) {
        jint uc = -1;
        uint16_t w1 = src[i];
        if ((w1 < 0xD800) || (w1 > 0xDFFF)) {
            uc = w1;
        } else if ((w1 >= 0xD800) && (w1 <= 0xDBFF)) {
            if (i < size - 1) {
                uint16_t w2 = src[i+1];
                if (w2 >= 0xDC00 && w2 <= 0xDFFF) {
                    uc = (((w1 & 0x3FF) << 10) | (w2 & 0x3FF)) + 0x10000;
                    i+=1;
                }
            }
        }
        if (uc == -1) {
            return JNI_TRUE;
        }
        if (uc < 0x80) {
            dst[sp++] = ((char)uc) & 0xFF;
        } else if (uc < 0x800) {
            dst[sp++] = ((char)(((uc >> 6) & 0x1F) | 0xC0)) & 0xFF;
            dst[sp++] = ((char)(((uc >> 0) & 0x3F) | 0x80)) & 0xFF;
        } else if (uc < 0x10000) {
            dst[sp++] = ((char)(((uc >>12) & 0x0F) | 0xE0)) & 0xFF;
            dst[sp++] = ((char)(((uc >> 6) & 0x3F) | 0x80)) & 0xFF;
            dst[sp++] = ((char)(((uc >> 0) & 0x3F) | 0x80)) & 0xFF;
        } else {
            dst[sp++] = ((char)(((uc >>18) & 0x07) | 0xF0)) & 0xFF;
            dst[sp++] = ((char)(((uc >>12) & 0x3F) | 0x80)) & 0xFF;
            dst[sp++] = ((char)(((uc >> 6) & 0x3F) | 0x80)) & 0xFF;
            dst[sp++] = ((char)(((uc >> 0) & 0x3F) | 0x80)) & 0xFF;
        }
    }    
    dst[sp] = '\0';
    if (out) {
        *out = sp;
    }

    return JNI_TRUE;
}

inline static jboolean UTF8toUTF16(JNIEnv *env, const char* src, jchar* dst, jsize size, jsize* out) {
    if (out) {
        *out = 0;
    }
    jint sp = 0, i;
    for (i = 0; i < size; i++) {
        jint uc = -1;
        uint8_t w1 = src[i];
        if (w1 <= 0x7F) {
            uc = w1;
        } else if ((w1 >= 0xC0) && (w1 <= 0xDF)) {
            if (i < size - 1) {
                uint8_t w2 = src[i+1];
                if ((w2 & 0xC0) == 0x80) {
                    uc = ((w1 & 0x1F) << 6) 
                        | ((w2 & 0x3F) << 0);
                    i+=1;
                }
            }
        } else if ((w1 >= 0xE0) && (w1 <= 0xEF)) {
            if (i < size - 2) {
                uint8_t w2 = src[i+1];
                uint8_t w3 = src[i+2];
                if ((w2 & 0xC0) == 0x80 
                    && (w3 & 0xC0) == 0x80) {
                    uc = ((w1 & 0x0F) << 12)
                        | ((w2 & 0x3F) << 6) 
                        | ((w3 & 0x3F) << 0);
                    i+=2;
                }
            }
        } else if ((w1 >= 0xF0) && (w1 <= 0xF7)) {
            if (i < size - 3) {
                uint8_t w2 = src[i+1];
                uint8_t w3 = src[i+2];
                uint8_t w4 = src[i+3];
                if ((w2 & 0xC0) == 0x80 
                    && (w3 & 0xC0) == 0x80
                    && (w4 & 0xC0) == 0x80) {
                    uc = ((w1 & 0x07) << 18) 
                        | ((w2 & 0x3F) << 12) 
                        | ((w3 & 0x3F) << 6) 
                        | ((w4 & 0x3F) << 0);
                    i+=3;
                }
            }
        }
        if (uc == -1) {
            return JNI_FALSE;
        }
        if (uc < 0x10000) {
            dst[sp++] = ((jchar)uc) & 0xFFFF;
        } else {
            uc -= 0x10000;
            dst[sp++] = ((jchar)(((uc >>10)& 0x3FF) | 0xD800)) & 0xFFFF;
            dst[sp++] = ((jchar)(((uc >> 0)& 0x3FF) | 0xDC00)) & 0xFFFF;
        }
    }
    dst[sp] = '\0';
    if (out) {
        *out = sp;
    }

    return JNI_TRUE;
}

inline static jobject bytesToObject(JNIEnv *env, const char* bytes, jsize length, jint mode) {
    if (!bytes) {
        return NULL;
    }

    //ByteArray
    if (mode == ARRAY) {
        return bytesToArray(env, bytes, length);
    }

    //ByteBuffer
    if (mode == BUFFER) {
        return (*env)->NewDirectByteBuffer(env, (void*)bytes, length);
    }
    
    //String
    if (mode == STRING_CESU8) {
        return (*env)->NewStringUTF(env, bytes);
    }

    if (mode == STRING_JUTF8) {
        static jmethodID mth = 0;
        if (!mth) {
            mth = (*env)->GetMethodID(
                env, strclass, "<init>", "([BLjava/nio/charset/Charset;)V");
        }

        jbyteArray array;    
        array = bytesToArray(env, bytes, length);
        if (!array) {
            return NULL;
        }

        return (*env)->NewObject(env, strclass, mth, array, strencoding);
    }

    //STRING C
    jchar* chars = malloc((length + 1) * sizeof(jchar));
    if (!chars) {
        throwex_outofmemory(env);
        return NULL;
    }

    jsize size;
    if (!UTF8toUTF16(env, bytes, chars, length, &size)) {
        free(chars);
        throwex_msg(env, "Bad UTF-8 coding!");
        return NULL;
    }

    jstring ret = (*env)->NewString(env, chars, size);
    free(chars);
    return ret;
}

inline static const char* objectToBytes(JNIEnv *env, jobject object, jsize* size, jint mode) {
    if (size) {
        *size = 0;
    }
    if (!object) {        
        return NULL;
    }

    //ByteArray
    if ((*env)->IsInstanceOf(env, object, arrclass)) {
        return arrayToBytes(env, object, size);
    }

    //ByteBuffer    
    if ((*env)->IsInstanceOf(env, object, bufclass)) {
        if (size) {
            *size = (*env)->GetDirectBufferCapacity(env, object) - 1;
        }
        return (*env)->GetDirectBufferAddress(env, object);
    }

    //String
    if (!(*env)->IsInstanceOf(env, object, strclass)) {
        throwex_msg(env, "object is not string");
        return NULL;
    }

    if (mode == STRING_CESU8) {
        const char* ret = (*env)->GetStringUTFChars(env, object, JNI_FALSE);
        if (size && ret) {
            *size = strlen(ret);
        }
        return ret;
    }

    if (mode == STRING_JUTF8) {
        static jmethodID mth = 0;
        if (!mth) {
            mth = (*env)->GetMethodID(
                env, strclass, "getBytes", "(Ljava/nio/charset/Charset;)[B");
        }
        
        jbyteArray array;
        array = (*env)->CallObjectMethod(env, object, mth, strencoding);
        if (!array) {
            return NULL;
        }

        return arrayToBytes(env, array, size);
    }

    //STRING C
    jsize length = (*env)->GetStringLength(env, object);
    char* ret = malloc(length * 4 + 1);
    if (!ret) {
        throwex_outofmemory(env);
        return NULL;
    }

    const jchar* chars = (*env)->GetStringCritical(env, object, JNI_FALSE);
    jboolean stat = UTF16toUTF8(env, chars, ret, length, size);
    (*env)->ReleaseStringCritical(env, object, chars);

    if (!stat) {
        free(ret);
        throwex_msg(env, "Bad UTF-16 encoding!");
        return NULL;        
    }

    return ret;
}

inline static void freeBytes(JNIEnv *env, jobject object, const char* bytes, jint mode) {
    if (!bytes) return;
    if (object) {
        if (mode == BUFFER) return;
        if (mode == STRING_CESU8) {
            (*env)->ReleaseStringUTFChars(env, object, bytes);
            return;
        }
    }
    free((void*)bytes);
}

inline static sqlite3 * gethandle(JNIEnv *env, jobject this)
{
    static jfieldID pointer = 0;
    if (!pointer) pointer = (*env)->GetFieldID(env, dbclass, "pointer", "J");

    return (sqlite3 *)toref((*env)->GetLongField(env, this, pointer));
}

inline static void sethandle(JNIEnv *env, jobject this, sqlite3 * ref)
{
    static jfieldID pointer = 0;
    if (!pointer) pointer = (*env)->GetFieldID(env, dbclass, "pointer", "J");

    (*env)->SetLongField(env, this, pointer, fromref(ref));
}


// User Defined Function SUPPORT ////////////////////////////////////

struct UDFData {
    JavaVM *vm;
    jobject func;
    struct UDFData *next;  // linked list of all UDFData instances
};

/* Returns the sqlite3_value for the given arg of the given function.
 * If 0 is returned, an exception has been thrown to report the reason. */
static sqlite3_value * tovalue(JNIEnv *env, jobject function, jint arg)
{
    jlong value_pntr = 0;
    jint numArgs = 0;
    static jfieldID func_value = 0,
                    func_args = 0;

    if (!func_value || !func_args) {
        func_value = (*env)->GetFieldID(env, fclass, "value", "J");
        func_args  = (*env)->GetFieldID(env, fclass, "args", "I");
    }

    // check we have any business being here
    if (arg  < 0) { throwex_msg(env, "negative arg out of range"); return 0; }
    if (!function) { throwex_msg(env, "inconstent function"); return 0; }

    value_pntr = (*env)->GetLongField(env, function, func_value);
    numArgs = (*env)->GetIntField(env, function, func_args);

    if (value_pntr == 0) { throwex_msg(env, "no current value"); return 0; }
    if (arg >= numArgs) { throwex_msg(env, "arg out of range"); return 0; }

    return ((sqlite3_value**)toref(value_pntr))[arg];
}

/* called if an exception occured processing xFunc */
static void xFunc_error(sqlite3_context *context, JNIEnv *env)
{
    jstring msg = 0;
    jsize size;
    const char *msg_bytes;

    jclass exclass = 0;
    static jmethodID exp_msg = 0;
    jthrowable ex = (*env)->ExceptionOccurred(env);

    (*env)->ExceptionClear(env);

    if (!exp_msg) {
        exclass = (*env)->FindClass(env, "java/lang/Throwable");
        exp_msg = (*env)->GetMethodID(
                env, exclass, "toString", "()Ljava/lang/String;");
    }

    msg = (jstring)(*env)->CallObjectMethod(env, ex, exp_msg);
    if (!msg) { sqlite3_result_error(context, "unknown error", 13); return; }

    msg_bytes = objectToBytes(env, msg, &size, STRING_CUTF8);
    if (!msg_bytes)
    {
        sqlite3_result_error_nomem(context); 
        return; 
    }

    sqlite3_result_error(context, msg_bytes, size);
    freeBytes(env, msg, msg_bytes, STRING_CUTF8);
}

/* used to call xFunc, xStep and xFinal */
static void xCall(
    sqlite3_context *context,
    int args,
    sqlite3_value** value,
    jobject func,
    jmethodID method)
{
    static jfieldID fld_context = 0,
                     fld_value = 0,
                     fld_args = 0;
    JNIEnv *env = 0;
    struct UDFData *udf = 0;

    udf = (struct UDFData*)sqlite3_user_data(context);
    assert(udf);
    (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);
    if (!func) func = udf->func;

    if (!fld_context || !fld_value || !fld_args) {
        fld_context = (*env)->GetFieldID(env, fclass, "context", "J");
        fld_value   = (*env)->GetFieldID(env, fclass, "value", "J");
        fld_args    = (*env)->GetFieldID(env, fclass, "args", "I");
    }

    (*env)->SetLongField(env, func, fld_context, fromref(context));
    (*env)->SetLongField(env, func, fld_value, value ? fromref(value) : 0);
    (*env)->SetIntField(env, func, fld_args, args);

    (*env)->CallVoidMethod(env, func, method);

    // check if xFunc threw an Exception
    if ((*env)->ExceptionCheck(env)) {
        xFunc_error(context, env);
    }

    (*env)->SetLongField(env, func, fld_context, 0);
    (*env)->SetLongField(env, func, fld_value, 0);
    (*env)->SetIntField(env, func, fld_args, 0);
}


void xFunc(sqlite3_context *context, int args, sqlite3_value** value)
{
    static jmethodID mth = 0;
    if (!mth) {
        JNIEnv *env;
        struct UDFData *udf = (struct UDFData*)sqlite3_user_data(context);
        (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);
        mth = (*env)->GetMethodID(env, fclass, "xFunc", "()V");
    }
    xCall(context, args, value, 0, mth);
}

void xStep(sqlite3_context *context, int args, sqlite3_value** value)
{
    JNIEnv *env;
    struct UDFData *udf;
    jobject *func = 0;
    static jmethodID mth = 0;
    static jmethodID clone = 0;

    if (!mth || !clone) {
        udf = (struct UDFData*)sqlite3_user_data(context);
        (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

        mth = (*env)->GetMethodID(env, aclass, "xStep", "()V");
        clone = (*env)->GetMethodID(env, aclass, "clone",
            "()Ljava/lang/Object;");
    }

    // clone the Function.Aggregate instance and store a pointer
    // in SQLite's aggregate_context (clean up in xFinal)
    func = sqlite3_aggregate_context(context, sizeof(jobject));
    if (!*func) {
        udf = (struct UDFData*)sqlite3_user_data(context);
        (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

        *func = (*env)->CallObjectMethod(env, udf->func, clone);
        *func = (*env)->NewGlobalRef(env, *func);
    }

    xCall(context, args, value, *func, mth);
}

void xInverse(sqlite3_context *context, int args, sqlite3_value** value)
{
    JNIEnv *env = 0;
    struct UDFData *udf = 0;
    jobject *func = 0;
    static jmethodID mth = 0;

    udf = (struct UDFData*)sqlite3_user_data(context);
    (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

    if (!mth) mth = (*env)->GetMethodID(env, wclass, "xInverse", "()V");

    func = sqlite3_aggregate_context(context, sizeof(jobject));
    assert(*func); // disaster

    xCall(context, args, value, *func, mth);
}

void xValue(sqlite3_context *context)
{
    JNIEnv *env = 0;
    struct UDFData *udf = 0;
    jobject *func = 0;
    static jmethodID mth = 0;

    udf = (struct UDFData*)sqlite3_user_data(context);
    (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

    if (!mth) mth = (*env)->GetMethodID(env, wclass, "xValue", "()V");

    func = sqlite3_aggregate_context(context, sizeof(jobject));
    assert(*func); // disaster

    xCall(context, 0, 0, *func, mth);
}

void xFinal(sqlite3_context *context)
{
    JNIEnv *env = 0;
    struct UDFData *udf = 0;
    jobject *func = 0;
    static jmethodID mth = 0;
    static jmethodID clone = 0;

    udf = (struct UDFData*)sqlite3_user_data(context);
    (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

    if (!mth) mth = (*env)->GetMethodID(env, aclass, "xFinal", "()V");

    func = sqlite3_aggregate_context(context, sizeof(jobject));
    // func may not have been allocated if xStep never ran
    if (!*func) {
        udf = (struct UDFData*)sqlite3_user_data(context);
        (*udf->vm)->AttachCurrentThread(udf->vm, (void **)&env, 0);

        clone = (*env)->GetMethodID(env, aclass, "clone",
            "()Ljava/lang/Object;");

        *func = (*env)->CallObjectMethod(env, udf->func, clone);
        *func = (*env)->NewGlobalRef(env, *func);
    }

    xCall(context, 0, 0, *func, mth);

    // clean up Function.Aggregate instance
    (*env)->DeleteGlobalRef(env, *func);
}

// INITIALISATION ///////////////////////////////////////////////////

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv* env = 0;

    if (JNI_OK != (*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_2))
        return JNI_ERR;

    jclass clazz = (*env)->FindClass(env, "java/nio/charset/Charset");
    if (!clazz) return JNI_ERR;
    jmethodID method = (*env)->GetStaticMethodID(env, clazz, "forName", 
            "(Ljava/lang/String;)Ljava/nio/charset/Charset;");
    if (!method) return JNI_ERR;
    jstring utf8 = (*env)->NewStringUTF(env, "UTF-8");    
    if (!utf8) return JNI_ERR;

    strencoding = (*env)->CallStaticObjectMethod(env, clazz, method, utf8);
    if (!strencoding) return JNI_ERR;
    strencoding = (*env)->NewWeakGlobalRef(env ,strencoding);

    strclass = (*env)->FindClass(env, "java/lang/String");
    if (!strclass) return JNI_ERR;
    strclass = (*env)->NewWeakGlobalRef(env, strclass);

    bufclass = (*env)->FindClass(env, "java/nio/ByteBuffer");
    if (!bufclass) return JNI_ERR;
    bufclass = (*env)->NewWeakGlobalRef(env, bufclass);

    arrclass = (*env)->FindClass(env, "[B");
    if (!arrclass) return JNI_ERR;
    arrclass = (*env)->NewWeakGlobalRef(env, arrclass);

    dbclass = (*env)->FindClass(env, "org/sqlite/core/NativeDB");
    if (!dbclass) return JNI_ERR;
    dbclass = (*env)->NewWeakGlobalRef(env, dbclass);

    fclass = (*env)->FindClass(env, "org/sqlite/Function");
    if (!fclass) return JNI_ERR;
    fclass = (*env)->NewWeakGlobalRef(env, fclass);

    aclass = (*env)->FindClass(env, "org/sqlite/Function$Aggregate");
    if (!aclass) return JNI_ERR;
    aclass = (*env)->NewWeakGlobalRef(env, aclass);

    wclass = (*env)->FindClass(env, "org/sqlite/Function$Window");
    if (!wclass) return JNI_ERR;
    wclass = (*env)->NewWeakGlobalRef(env, wclass);

    pclass = (*env)->FindClass(env, "org/sqlite/core/DB$ProgressObserver");
    if(!pclass) return JNI_ERR;
    pclass = (*env)->NewWeakGlobalRef(env, pclass);

    phandleclass = (*env)->FindClass(env, "org/sqlite/ProgressHandler");
    if(!phandleclass) return JNI_ERR;
    phandleclass = (*env)->NewWeakGlobalRef(env, phandleclass);

    return JNI_VERSION_1_2;
}

// FINALIZATION

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
    JNIEnv* env = 0;

    if (JNI_OK != (*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_2))
        return;
    if (strencoding) (*env)->DeleteWeakGlobalRef(env ,strencoding);
    if (arrclass) (*env)->DeleteWeakGlobalRef(env, arrclass);
    if (bufclass) (*env)->DeleteWeakGlobalRef(env, bufclass);
    if (strclass) (*env)->DeleteWeakGlobalRef(env, strclass);

    if (dbclass) (*env)->DeleteWeakGlobalRef(env, dbclass);
    if (fclass) (*env)->DeleteWeakGlobalRef(env, fclass);
    if (aclass) (*env)->DeleteWeakGlobalRef(env, aclass);
    if (wclass) (*env)->DeleteWeakGlobalRef(env, wclass);
    if (pclass) (*env)->DeleteWeakGlobalRef(env, pclass);
    if (phandleclass) (*env)->DeleteWeakGlobalRef(env, phandleclass);
}

// WRAPPERS for sqlite_* functions //////////////////////////////////

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_shared_1cache(
        JNIEnv *env, jobject this, jboolean enable)
{
    return sqlite3_enable_shared_cache(enable ? 1 : 0);
}


JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_enable_1load_1extension(
        JNIEnv *env, jobject this, jboolean enable)
{
    sqlite3 *db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_enable_load_extension(db, enable ? 1 : 0);
}


JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB__1open0(
        JNIEnv *env, jobject this, jobject file, jint flags, jint mode)
{
    sqlite3 *db;
    int ret;
    const char *file_bytes;

    db = gethandle(env, this);
    if (db) {
        throwex_msg(env, "DB already open");
        sqlite3_close(db);
        return;
    }

    file_bytes = objectToBytes(env, file, NULL, mode);
    if (!file_bytes) return;

    ret = sqlite3_open_v2(file_bytes, &db, flags, NULL);
    freeBytes(env, file, file_bytes, mode);

    sethandle(env, this, db);
    if (ret != SQLITE_OK) {
        ret = sqlite3_extended_errcode(db);
        throwex_errorcode(env, this, ret);
        sethandle(env, this, 0); // The handle is needed for throwex_errorcode
        sqlite3_close(db);
        return;
    }

    // Ignore failures, as we can tolerate regular result codes.
    (void) sqlite3_extended_result_codes(db, 1);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB__1close(
        JNIEnv *env, jobject this)
{
    sqlite3 *db = gethandle(env, this);
    if (db)
    {
        if (sqlite3_close(db) != SQLITE_OK)
        {
            throwex(env, this);
        }
        sethandle(env, this, 0);
    }
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_interrupt(JNIEnv *env, jobject this)
{
    sqlite3 *db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return;
    }

    sqlite3_interrupt(db);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_busy_1timeout(
    JNIEnv *env, jobject this, jint ms)
{
    sqlite3 *db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return;
    }

    sqlite3_busy_timeout(db, ms);
}

struct BusyHandlerContext {
    JavaVM * vm;
    jmethodID methodId;
    jobject obj;
};

static struct BusyHandlerContext busyHandlerContext;

int busyHandlerCallBack(void * ctx, int nbPrevInvok) {
    
    JNIEnv *env = 0;
    (*busyHandlerContext.vm)->AttachCurrentThread(busyHandlerContext.vm, (void **)&env, 0);

    return (*env)->CallIntMethod(   env, 
                                    busyHandlerContext.obj, 
                                    busyHandlerContext.methodId, 
                                    nbPrevInvok);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_busy_1handler(
    JNIEnv *env, jobject this, jobject busyHandler)
{
    sqlite3 *db;

    (*env)->GetJavaVM(env, &busyHandlerContext.vm);
    
    if (busyHandler != NULL) {
        busyHandlerContext.obj = (*env)->NewGlobalRef(env, busyHandler);
        busyHandlerContext.methodId = (*env)->GetMethodID(  env, 
                                                            (*env)->GetObjectClass(env, busyHandlerContext.obj), 
                                                            "callback", 
                                                            "(I)I");
    }

    db = gethandle(env, this);
    if (!db){
        throwex_db_closed(env);
        return;
    }
    
    if (busyHandler != NULL) {
        sqlite3_busy_handler(db, &busyHandlerCallBack, NULL);
    } else {
        sqlite3_busy_handler(db, NULL, NULL);
    }
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_prepare0(
        JNIEnv *env, jobject this, jobject sql, jint mode)
{
    sqlite3* db;
    sqlite3_stmt* stmt;
    const char* sql_bytes;
    jsize size;
    int status;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return 0;
    }

    sql_bytes = objectToBytes(env, sql, &size, mode);
    if (!sql_bytes) return fromref(0);

    status = sqlite3_prepare_v2(db, sql_bytes, size, &stmt, 0);
    freeBytes(env, sql, sql_bytes, mode);

    if (status != SQLITE_OK) {
        throwex_errorcode(env, this, status);
        return fromref(0);
    }
    return fromref(stmt);
}


JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB__1exec0(
        JNIEnv *env, jobject this, jobject sql, jint mode)
{
    sqlite3* db;
    const char* sql_bytes;
    int status;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_errorcode(env, this, SQLITE_MISUSE);
        return SQLITE_MISUSE;
    }

    sql_bytes = objectToBytes(env, sql, NULL, mode);
    if (!sql_bytes)
    {
        return SQLITE_ERROR;
    }

    status = sqlite3_exec(db, sql_bytes, 0, 0, NULL);
    freeBytes(env, sql, sql_bytes, mode);

    if (status != SQLITE_OK) {
        throwex_errorcode(env, this, status);
    }

    return status;
}


JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_errmsg0(JNIEnv *env, jobject this, jint mode)
{
    sqlite3 *db;
    const char *str;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return NULL;
    }
    
    str = (const char*) sqlite3_errmsg(db);
    if (!str) return NULL;

    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_libversion0(
        JNIEnv *env, jobject this, jint mode)
{
    const char* version = sqlite3_libversion();

    return bytesToObject(env, version, strlen(version), mode);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_changes(
        JNIEnv *env, jobject this)
{
    sqlite3 *db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return 0;
    }

    return sqlite3_changes(db);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_total_1changes(
        JNIEnv *env, jobject this)
{
    sqlite3 *db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return 0;
    }

    return sqlite3_total_changes(db);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_finalize(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_finalize(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_step(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_step(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_reset(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_reset(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_clear_1bindings(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_clear_bindings(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1parameter_1count(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_bind_parameter_count(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1count(
        JNIEnv *env, jobject this, jlong stmt)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_column_count(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1type(
        JNIEnv *env, jobject this, jlong stmt, jint col)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_column_type(toref(stmt), col);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1decltype0(
        JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str;

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    str = (const char*) sqlite3_column_decltype(toref(stmt), col);
    if (!str) return NULL;

    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1table_1name0(
        JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str;

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    str = sqlite3_column_table_name(toref(stmt), col);
    if (!str) return NULL;

    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1name0(
        JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str;

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    str = sqlite3_column_name(toref(stmt), col);
    if (!str) return NULL;

    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1text0(
        JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    sqlite3 *db;
    const char *bytes;
    jsize size;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return NULL;
    }

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    bytes = (const char*) sqlite3_column_text(toref(stmt), col);

    if (!bytes && sqlite3_errcode(db) == SQLITE_NOMEM)
    {
        throwex_outofmemory(env);
        return NULL;
    }
    size = sqlite3_column_bytes(toref(stmt), col);
    return bytesToObject(env, bytes, size, mode);
}

JNIEXPORT jbyteArray JNICALL Java_org_sqlite_core_NativeDB_column_1blob(
        JNIEnv *env, jobject this, jlong stmt, jint col)
{
    sqlite3 *db;
    int type;
    int length;
    const void *blob;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return NULL;
    }

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    // The value returned by sqlite3_column_type() is only meaningful if no type conversions have occurred
    type = sqlite3_column_type(toref(stmt), col);
    blob = sqlite3_column_blob(toref(stmt), col);
    if (!blob && sqlite3_errcode(db) == SQLITE_NOMEM)
    {
        throwex_outofmemory(env);
        return NULL;
    }
    if (!blob) {
        if (type == SQLITE_NULL) {
            return NULL;
        }
        else {
            // The return value from sqlite3_column_blob() for a zero-length BLOB is a NULL pointer.
            return bytesToArray(env, '\0', 0);
        }
    }  

    length = sqlite3_column_bytes(toref(stmt), col);
    return bytesToArray(env, (const char*) blob, length);
}

JNIEXPORT jdouble JNICALL Java_org_sqlite_core_NativeDB_column_1double(
        JNIEnv *env, jobject this, jlong stmt, jint col)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return 0;
    }

    return sqlite3_column_double(toref(stmt), col);
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_column_1long(
        JNIEnv *env, jobject this, jlong stmt, jint col)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return 0;
    }

    return sqlite3_column_int64(toref(stmt), col);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1int(
        JNIEnv *env, jobject this, jlong stmt, jint col)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return 0;
    }

    return sqlite3_column_int(toref(stmt), col);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1null(
        JNIEnv *env, jobject this, jlong stmt, jint pos)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_bind_null(toref(stmt), pos);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1int(
        JNIEnv *env, jobject this, jlong stmt, jint pos, jint v)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_bind_int(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1long(
        JNIEnv *env, jobject this, jlong stmt, jint pos, jlong v)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_bind_int64(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1double(
        JNIEnv *env, jobject this, jlong stmt, jint pos, jdouble v)
{
    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    return sqlite3_bind_double(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1text0(
        JNIEnv *env, jobject this, jlong stmt, jint pos, jobject v, jint mode)
{
    int rc;
    const char* v_bytes;
    jsize size;

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    v_bytes = objectToBytes(env, v, &size, mode);
    if (!v_bytes) return SQLITE_ERROR;

    rc = sqlite3_bind_text(toref(stmt), pos, v_bytes, size, SQLITE_TRANSIENT);
    freeBytes(env, v, v_bytes, mode);

    return rc;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1blob(
        JNIEnv *env, jobject this, jlong stmt, jint pos, jbyteArray v)
{
    jint rc;
    void *bytes;
    jsize size;

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return SQLITE_MISUSE;
    }

    bytes = arrayToBytes(env, v, &size);
    if (!bytes) 
    {
        return SQLITE_ERROR;
    }

    rc = sqlite3_bind_blob(toref(stmt), pos, bytes, size, SQLITE_TRANSIENT);

    freeBytes(env, v, bytes, STRING_CUTF8);

    return rc;
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1null(
        JNIEnv *env, jobject this, jlong context)
{
    if (!context) return;
    sqlite3_result_null(toref(context));
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1text0(
        JNIEnv *env, jobject this, jlong context, jobject value, jint mode)
{
    const char* value_bytes;
    jsize size;
    
    if (!context) return;
    if (value == NULL) {
        sqlite3_result_null(toref(context)); 
        return;
    }

    value_bytes = objectToBytes(env, value, &size, mode);
    if (!value_bytes)
    {
        sqlite3_result_error_nomem(toref(context));
        return;
    }

    sqlite3_result_text(toref(context), value_bytes, size, SQLITE_TRANSIENT);
    freeBytes(env, value, value_bytes, mode);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1blob(
        JNIEnv *env, jobject this, jlong context, jbyteArray value)
{
    jbyte *bytes;
    jsize size;

    if (!context) return;
    if (value == NULL) { sqlite3_result_null(toref(context)); return; }

    bytes = arrayToBytes(env, value, &size);
    if (!bytes) 
    {
        sqlite3_result_null(toref(context));
    } else {
        sqlite3_result_blob(toref(context), bytes, size, SQLITE_TRANSIENT);
        freeBytes(env, NULL, bytes, STRING_CUTF8);
    }
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1double(
        JNIEnv *env, jobject this, jlong context, jdouble value)
{
    if (!context) return;
    sqlite3_result_double(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1long(
        JNIEnv *env, jobject this, jlong context, jlong value)
{
    if (!context) return;
    sqlite3_result_int64(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1int(
        JNIEnv *env, jobject this, jlong context, jint value)
{
    if (!context) return;
    sqlite3_result_int(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1error0(
        JNIEnv *env, jobject this, jlong context, jobject err, jint mode)
{
    const char* err_bytes;
    jsize size;

    if (!context) return;

    err_bytes = objectToBytes(env, err, &size, mode);
    if (!err_bytes)
    {
        sqlite3_result_error_nomem(toref(context));
        return;
    }

    sqlite3_result_error(toref(context), err_bytes, size);
    freeBytes(env, err, err_bytes, mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_value_1text0(
        JNIEnv *env, jobject this, jobject f, jint arg, jint mode)
{
    const char* bytes;
    jsize size;

    sqlite3_value *value = tovalue(env, f, arg);
    if (!value) return NULL;

    bytes = (const char*) sqlite3_value_text(value);
    size = sqlite3_value_bytes(value);

    return bytesToObject(env, bytes, size, mode);
}

JNIEXPORT jbyteArray JNICALL Java_org_sqlite_core_NativeDB_value_1blob(
        JNIEnv *env, jobject this, jobject f, jint arg)
{
    int length;
    const void *blob;

    sqlite3_value *value = tovalue(env, f, arg);
    if (!value) return NULL;

    blob = sqlite3_value_blob(value);
    if (!blob) return NULL;

    length = sqlite3_value_bytes(value);
    
    return bytesToArray(env, (const char*)blob, length);
}

JNIEXPORT jdouble JNICALL Java_org_sqlite_core_NativeDB_value_1double(
        JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_double(value) : 0;
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_value_1long(
        JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_int64(value) : 0;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_value_1int(
        JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_int(value) : 0;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_value_1type(
        JNIEnv *env, jobject this, jobject func, jint arg)
{
    return sqlite3_value_type(tovalue(env, func, arg));
}


JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_create_1function0(
        JNIEnv *env, jobject this, jobject name, jobject func, jint nArgs, jint flags, jint mode)
{
    jint ret = 0;
    const char *name_bytes;
    int isAgg = 0, isWindow = 0;

    static jfieldID udfdatalist = 0;
    struct UDFData *udf = malloc(sizeof(struct UDFData));

    if (!udf) { throwex_outofmemory(env); return 0; }

    if (!udfdatalist)
        udfdatalist = (*env)->GetFieldID(env, dbclass, "udfdatalist", "J");

    isAgg = (*env)->IsInstanceOf(env, func, aclass);
    isWindow = (*env)->IsInstanceOf(env, func, wclass);
    udf->func = (*env)->NewGlobalRef(env, func);
    (*env)->GetJavaVM(env, &udf->vm);

    // add new function def to linked list
    udf->next = toref((*env)->GetLongField(env, this, udfdatalist));
    (*env)->SetLongField(env, this, udfdatalist, fromref(udf));

    name_bytes = objectToBytes(env, name, NULL, mode);
    if (!name_bytes) 
    { 
        throwex_outofmemory(env); 
        return 0;
    }

    if (isAgg) {
        ret = sqlite3_create_window_function(
                gethandle(env, this),
                name_bytes,            // function name
                nArgs,                 // number of args
                SQLITE_UTF8 | flags,  // preferred chars
                udf,
                &xStep,
                &xFinal,
                isWindow ? &xValue : 0,
                isWindow ? &xInverse : 0,
                0
        );
    } else {
        ret = sqlite3_create_function(
                gethandle(env, this),
                name_bytes,            // function name
                nArgs,                 // number of args
                SQLITE_UTF8 | flags,  // preferred chars
                udf,
                &xFunc,
                0,
                0
        );
    }

    freeBytes(env, name, name_bytes, mode);

    return ret;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_destroy_1function0(
        JNIEnv *env, jobject this, jobject name, jint nArgs, jint mode)
{
    jint ret = 0;
    const char* name_bytes;

    name_bytes = objectToBytes(env, name, NULL, mode);
    if (!name_bytes) 
    {
        throwex_outofmemory(env); 
        return 0; 
    }
    
    ret = sqlite3_create_function(
        gethandle(env, this), name_bytes, nArgs, SQLITE_UTF8, 0, 0, 0, 0
    );
    
    freeBytes(env, name, name_bytes, mode);

    return ret;
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_free_1functions(
        JNIEnv *env, jobject this)
{
    // clean up all the malloc()ed UDFData instances using the
    // linked list stored in DB.udfdatalist
    jfieldID udfdatalist;
    struct UDFData *udf, *udfpass;

    udfdatalist = (*env)->GetFieldID(env, dbclass, "udfdatalist", "J");
    udf = toref((*env)->GetLongField(env, this, udfdatalist));
    (*env)->SetLongField(env, this, udfdatalist, 0);

    while (udf) {
        udfpass = udf->next;
        (*env)->DeleteGlobalRef(env, udf->func);
        free(udf);
        udf = udfpass;
    }
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_limit(JNIEnv *env, jobject this, jint id, jint value)
{
    sqlite3* db;

    db = gethandle(env, this);

    if (!db)
    {
        throwex_db_closed(env);
        return 0;
    }

    return sqlite3_limit(db, id, value);
}

// COMPOUND FUNCTIONS ///////////////////////////////////////////////

JNIEXPORT jobjectArray JNICALL Java_org_sqlite_core_NativeDB_column_1metadata(
        JNIEnv *env, jobject this, jlong stmt)
{
    const char *zTableName, *zColumnName;
    int pNotNull, pPrimaryKey, pAutoinc, i, colCount;
    jobjectArray array;
    jbooleanArray colData;
    jboolean* colDataRaw;
    sqlite3 *db;
    sqlite3_stmt *dbstmt;

    db = gethandle(env, this);
    if (!db)
    {
        throwex_db_closed(env);
        return NULL;
    }

    if (!stmt)
    {
        throwex_stmt_finalized(env);
        return NULL;
    }

    dbstmt = toref(stmt);

    colCount = sqlite3_column_count(dbstmt);
    array = (*env)->NewObjectArray(
        env, colCount, (*env)->FindClass(env, "[Z"), NULL) ;
    if (!array) { throwex_outofmemory(env); return 0; }

    colDataRaw = (jboolean*)malloc(3 * sizeof(jboolean));
    if (!colDataRaw) { throwex_outofmemory(env); return 0; }

    for (i = 0; i < colCount; i++) {
        // load passed column name and table name
        zColumnName = sqlite3_column_name(dbstmt, i);
        zTableName  = sqlite3_column_table_name(dbstmt, i);

        pNotNull = 0;
        pPrimaryKey = 0;
        pAutoinc = 0;

        // request metadata for column and load into output variables
        if (zTableName && zColumnName) {
            sqlite3_table_column_metadata(
                db, 0, zTableName, zColumnName,
                0, 0, &pNotNull, &pPrimaryKey, &pAutoinc
            );
        }

        // load relevant metadata into 2nd dimension of return results
        colDataRaw[0] = pNotNull;
        colDataRaw[1] = pPrimaryKey;
        colDataRaw[2] = pAutoinc;

        colData = (*env)->NewBooleanArray(env, 3);
        if (!colData) { throwex_outofmemory(env); return 0; }

        (*env)->SetBooleanArrayRegion(env, colData, 0, 3, colDataRaw);
        (*env)->SetObjectArrayElement(env, array, i, colData);
    }

    free(colDataRaw);

    return array;
}

// backup function

void reportProgress(JNIEnv* env, jobject func, int remaining, int pageCount) {

  static jmethodID mth = 0;
  if (!mth) {
      mth = (*env)->GetMethodID(env, pclass, "progress", "(II)V");
  }

  if(!func) 
    return;

  (*env)->CallVoidMethod(env, func, mth, remaining, pageCount);
}


/*
** Perform an online backup of database pDb to the database file named
** by zFilename. This function copies 5 database pages from pDb to
** zFilename, then unlocks pDb and sleeps for 250 ms, then repeats the
** process until the entire database is backed up.
** 
** The third argument passed to this function must be a pointer to a progress
** function. After each set of 5 pages is backed up, the progress function
** is invoked with two integer parameters: the number of pages left to
** copy, and the total number of pages in the source file. This information
** may be used, for example, to update a GUI progress bar.
**
** While this function is running, another thread may use the database pDb, or
** another process may access the underlying database file via a separate 
** connection.
**
** If the backup process is successfully completed, SQLITE_OK is returned.
** Otherwise, if an error occurs, an SQLite error code is returned.
*/

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_backup0(
  JNIEnv *env, jobject this, 
  jobject zDBName,
  jobject zFilename,       /* Name of file to back up to */
  jobject observer,            /* Progress function to invoke */     
  jint mode
)
{
#if SQLITE_VERSION_NUMBER >= 3006011
  int rc;                     /* Function return code */
  sqlite3* pDb;               /* Database to back up */
  sqlite3* pFile;             /* Database connection opened on zFilename */
  sqlite3_backup *pBackup;    /* Backup handle used to copy data */
  const char *dFileName;
  const char *dDBName;

  pDb = gethandle(env, this);
  if (!pDb)
  {
    throwex_db_closed(env);
    return SQLITE_MISUSE;
  }

  dFileName = objectToBytes(env, zFilename, NULL, mode);
  if (!dFileName)
  {
    return SQLITE_NOMEM;
  }

  dDBName = objectToBytes(env, zDBName, NULL, mode);
  if (!dDBName)
  {
    freeBytes(env, zFilename, dFileName, mode);
    return SQLITE_NOMEM;
  }

  /* Open the database file identified by dFileName. */
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  if (sqlite3_strnicmp(dFileName, "file:", 5) == 0) {
    flags |= SQLITE_OPEN_URI;
  }
  rc = sqlite3_open_v2(dFileName, &pFile, flags, NULL);

  if( rc==SQLITE_OK ){

    /* Open the sqlite3_backup object used to accomplish the transfer */
    pBackup = sqlite3_backup_init(pFile, "main", pDb, dDBName);
    if( pBackup ){
      while((rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK ){}

      /* Release resources allocated by backup_init(). */
      (void)sqlite3_backup_finish(pBackup);
    }
    rc = sqlite3_errcode(pFile);
  }

  /* Close the database connection opened on database file zFilename
  ** and return the result of this function. */
  (void)sqlite3_close(pFile);

  freeBytes(env, zDBName, dDBName, mode);
  freeBytes(env, zFilename, dFileName, mode);

  return rc;
#else
  return SQLITE_INTERNAL;
#endif
} 

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_restore0(
  JNIEnv *env, jobject this, 
  jobject zDBName,
  jobject zFilename,         /* Name of file to back up to */
  jobject observer,              /* Progress function to invoke */
  jint mode
)
{
#if SQLITE_VERSION_NUMBER >= 3006011
  int rc;                     /* Function return code */
  sqlite3* pDb;               /* Database to back up */
  sqlite3* pFile;             /* Database connection opened on zFilename */
  sqlite3_backup *pBackup;    /* Backup handle used to copy data */
  const char *dFileName;
  const char *dDBName;
  int nTimeout = 0;

  pDb = gethandle(env, this);
  if (!pDb)
  {
    throwex_db_closed(env);
    return SQLITE_MISUSE;
  }

  dFileName = objectToBytes(env, zFilename, NULL, mode);
  if (!dFileName)
  {
    return SQLITE_NOMEM;
  }

  dDBName = objectToBytes(env, zDBName, NULL, mode);
  if (!dDBName)
  {
    freeBytes(env, zFilename, dFileName, mode);
    return SQLITE_NOMEM;
  }

  /* Open the database file identified by dFileName. */
  int flags = SQLITE_OPEN_READONLY;
  if (sqlite3_strnicmp(dFileName, "file:", 5) == 0) {
    flags |= SQLITE_OPEN_URI;
  }
  rc = sqlite3_open_v2(dFileName, &pFile, flags, NULL);

  if( rc==SQLITE_OK ){

    /* Open the sqlite3_backup object used to accomplish the transfer */
    pBackup = sqlite3_backup_init(pDb, dDBName, pFile, "main");
    if( pBackup ){
        while( (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK
              || rc==SQLITE_BUSY  ){
              if( rc==SQLITE_BUSY ){
                if( nTimeout++ >= 3 ) break;
                sqlite3_sleep(100);
            }
        }
      /* Release resources allocated by backup_init(). */
      (void)sqlite3_backup_finish(pBackup);
    }
    rc = sqlite3_errcode(pFile);
  }

  /* Close the database connection opened on database file zFilename
  ** and return the result of this function. */
  (void)sqlite3_close(pFile);

  freeBytes(env, zDBName, dDBName, mode);
  freeBytes(env, zFilename, dFileName, mode);

  return rc;
#else
  return SQLITE_INTERNAL;
#endif
}


// Progress handler

struct ProgressHandlerContext {
    JavaVM *vm;
    jmethodID mth;
    jobject phandler;
};

static struct ProgressHandlerContext progress_handler_context;

int progress_handler_function(void *ctx) {
    JNIEnv *env = 0;
    jint rv;
    (*progress_handler_context.vm)->AttachCurrentThread(progress_handler_context.vm, (void **)&env, 0);
    rv = (*env)->CallIntMethod(env, progress_handler_context.phandler, progress_handler_context.mth);
    return rv;
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_register_1progress_1handler(
  JNIEnv *env,
  jobject this,
  jint vmCalls,
  jobject progressHandler
)
{
    progress_handler_context.mth = (*env)->GetMethodID(env, phandleclass, "progress", "()I");
    progress_handler_context.phandler = (*env)->NewGlobalRef(env, progressHandler);
    (*env)->GetJavaVM(env, &progress_handler_context.vm);
    sqlite3_progress_handler(gethandle(env, this), vmCalls, &progress_handler_function, NULL);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_clear_1progress_1handler(
  JNIEnv *env,
  jobject this
)
{
    sqlite3_progress_handler(gethandle(env, this), 0, NULL, NULL);
    (*env)->DeleteGlobalRef(env, progress_handler_context.phandler);
}

// Update hook

struct UpdateHandlerContext {
    JavaVM *vm;
    jmethodID method;
    jobject handler;
};

static struct UpdateHandlerContext update_handler_context;


void update_hook(void *context, int type, char const *database, char const *table, sqlite3_int64 row) {
    JNIEnv *env = 0;
    (*update_handler_context.vm)->AttachCurrentThread(update_handler_context.vm, (void **)&env, 0);

    jstring tableString = bytesToObject(env, table, strlen(table), STRING_CUTF8);
    jstring databaseString = bytesToObject(env, database, strlen(database), STRING_CUTF8);
    (*env)->CallVoidMethod(env, update_handler_context.handler, update_handler_context.method, type, databaseString, tableString, row);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_set_1update_1listener(JNIEnv *env, jobject this, jboolean enabled) {
    if (enabled) {
        update_handler_context.method = (*env)->GetMethodID(env, dbclass, "onUpdate", "(ILjava/lang/String;Ljava/lang/String;J)V");
        update_handler_context.handler = (*env)->NewGlobalRef(env, this);
        (*env)->GetJavaVM(env, &update_handler_context.vm);
        sqlite3_update_hook(gethandle(env, this), &update_hook, NULL);
    } else {
        sqlite3_update_hook(gethandle(env, this), NULL, NULL);
        (*env)->DeleteGlobalRef(env, update_handler_context.handler);
    }
}

// Commit hook

struct CommitHandlerContext {
    JavaVM *vm;
    jmethodID method;
    jobject handler;
};

static struct CommitHandlerContext commit_handler_context;

int commit_hook(void *context) {
    JNIEnv *env = 0;
    (*commit_handler_context.vm)->AttachCurrentThread(commit_handler_context.vm, (void **)&env, 0);
    (*env)->CallVoidMethod(env, commit_handler_context.handler, commit_handler_context.method, 1);
    return 0;
}

void rollback_hook(void *context) {
    JNIEnv *env = 0;
    (*commit_handler_context.vm)->AttachCurrentThread(commit_handler_context.vm, (void **)&env, 0);
    (*env)->CallVoidMethod(env, commit_handler_context.handler, commit_handler_context.method, 0);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_set_1commit_1listener(JNIEnv *env, jobject this, jboolean enabled) {
    if (enabled) {
        commit_handler_context.method  = (*env)->GetMethodID(env, dbclass, "onCommit", "(Z)V");
        commit_handler_context.handler = (*env)->NewGlobalRef(env, this);
        (*env)->GetJavaVM(env, &commit_handler_context.vm);
        sqlite3_commit_hook(gethandle(env, this), &commit_hook, NULL);
        sqlite3_rollback_hook(gethandle(env, this), &rollback_hook, NULL);
    }  else {
        sqlite3_commit_hook(gethandle(env, this), NULL, NULL);
        sqlite3_update_hook(gethandle(env, this), NULL, NULL);
        (*env)->DeleteGlobalRef(env, commit_handler_context.handler);
    }
}