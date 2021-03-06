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

#ifdef SQLITE_JDBC_MEMORY
    #define MEMORY_REALLOC sqlite3_realloc
    #define MEMORY_MALLOC sqlite3_malloc
    #define MEMORY_FREE sqlite3_free
#else
    #define MEMORY_REALLOC realloc
    #define MEMORY_MALLOC malloc
    #define MEMORY_FREE free
#endif    

#ifndef SQLITE_JDBC_MAX_ALLOCA
    #define SQLITE_JDBC_MAX_ALLOCA 32768
#endif

typedef enum {
    ARRAY = 1, STRING
} SQLITEJDBC_STRING_CODING;

#define toref(x) (void *)(x)
#define fromref(x) (jlong)(x)

static void throwex_code(JNIEnv *env, jint code)
{
    static jmethodID mth = 0;
    if (!mth) {
        mth = (*env)->GetStaticMethodID(env, dbclass, "throwex", "(ILjava/lang/String;)V");
    }

    const char* msg = sqlite3_errstr(code);
    (*env)->CallStaticVoidMethod(env, dbclass, mth, code, (*env)->NewStringUTF(env, msg));
}

static void throwex_msg(JNIEnv *env, const char *str)
{
    static jmethodID mth = 0;
    if (!mth) {
        mth = (*env)->GetStaticMethodID(env, dbclass, "throwex", "(Ljava/lang/String;)V");
    }

    jstring msg = (*env)->NewStringUTF(env, str);
    (*env)->CallStaticVoidMethod(env, dbclass, mth, msg);
}

static void throwex_outofmemory(JNIEnv *env)
{
    throwex_msg(env, "Out of memory");
}

static jbyteArray bytesToArray(JNIEnv *env, const char* bytes, jsize length) {
    jbyteArray array = (*env)->NewByteArray(env, length);
    if (!array) {
        throwex_outofmemory(env);
        return NULL;
    }
    if (length) {
        (*env)->SetByteArrayRegion(env, array, 0, length, bytes);
    }
    return array;
}

static jboolean UTF16toUTF8(JNIEnv *env, const jchar* src, char* dst, jsize size, jsize* out) {
    if (out) *out = 0;
    jint sp, i;
    for (sp = 0, i = 0; i < size; ) {
        uint16_t w1 = src[i++];
        if (w1 < 0x80) {
            dst[sp++] = w1;
        } else if (w1 < 0x800) {
            dst[sp++] = ((w1 >> 6) & 0x1F) ^ 0xC0;
            dst[sp++] = ((w1 >> 0) & 0x3F) ^ 0x80;
        } else if ((w1 < 0xD800) || (w1 > 0xDFFF)) {
            dst[sp++] = ((w1 >>12) & 0x0F) ^ 0xE0;
            dst[sp++] = ((w1 >> 6) & 0x3F) ^ 0x80;
            dst[sp++] = ((w1 >> 0) & 0x3F) ^ 0x80;
        } else if (w1 < 0xDC00) {
            if (i == size) return JNI_FALSE;
            uint16_t w2 = src[i++];
            // if (w2 < 0xDC00 || w2 > 0xDFFF) return JNI_FALSE;
            jint uc = (((w1 & 0x3FF) << 10) ^ (w2 & 0x3FF)) + 0x10000;
            dst[sp++] = ((uc >>18) & 0x07) ^ 0xF0;
            dst[sp++] = ((uc >>12) & 0x3F) ^ 0x80;
            dst[sp++] = ((uc >> 6) & 0x3F) ^ 0x80;
            dst[sp++] = ((uc >> 0) & 0x3F) ^ 0x80;
        } else {
            return JNI_FALSE;
        }
    }    
    dst[sp] = '\0';
    if (out) *out = sp;

    return JNI_TRUE;
}

static jboolean UTF8toUTF16(JNIEnv *env, const char* src, jchar* dst, jsize size, jsize* out) {
    if (out)  *out = 0;
    jint sp, i;
    for (sp = 0, i = 0; i < size; ) {
        uint8_t w1 = src[i++];
        if (w1 < 0x80) {
            dst[sp++] = (jchar)w1;
        } else if (w1 < 0xE0) {
            if ((w1 < 0xC0) || (i == size)) return JNI_FALSE;
            uint8_t w2 = src[i++] & 0x3F;
            // if ((w2 & 0xC0) ^ 0x80) return JNI_FALSE;
            dst[sp++] = (jchar)(((w1 & 0x1F) << 6) ^ w2);
        } else if (w1 < 0xF0) {
            if (i + 1 == size) return JNI_FALSE;
            uint8_t w2 = src[i++] & 0x3F, w3 = src[i++] & 0x3F;
            // if (((w2 & 0xC0) ^ 0x80) || ((w3 & 0xC0) ^ 0x80)) return JNI_FALSE;
            dst[sp++] = (jchar)(((w1 & 0x0F) << 12) ^ (w2 << 6) ^ w3);
        } else if (w1 < 0xF8) {
            if (i + 2 == size) return JNI_FALSE;
            uint8_t w2 = src[i++] & 0x3F, w3 = src[i++] & 0x3F, w4 = src[i++] & 0x3F;
            // if (((w2 & 0xC0) ^ 0x80) || ((w3 & 0xC0) ^ 0x80) || ((w4 & 0xC0) ^ 0x80)) return JNI_FALSE;
            jint uc = (((w1 & 0x07) << 18) ^ (w2 << 12) ^ (w3 << 6) ^ w4) - 0x10000;
            dst[sp++] = (jchar)(((uc >>10) & 0x3FF) ^ 0xD800);
            dst[sp++] = (jchar)(((uc >> 0) & 0x3FF) ^ 0xDC00);
        } else {
            return JNI_FALSE;
        }
    }
    if (out) *out = sp;

    return JNI_TRUE;
}

static jobject bytesToObject(JNIEnv *env, const char* bytes, jsize length, jint mode) {
    if (!bytes) return NULL;

    if (mode == ARRAY) {
        return bytesToArray(env, bytes, length);
    }

    //STRING C
#ifdef SQLITE_USE_ALLOCA
    if (length < (SQLITE_JDBC_MAX_ALLOCA >> 1)) {
        jchar chars[length];
        jsize size;
        if (!UTF8toUTF16(env, bytes, chars, length, &size)) {
            throwex_msg(env, "Bad UTF-8 coding!");
            return NULL;
        }
        return (*env)->NewString(env, chars, size);
    }
#endif
    jchar* chars = MEMORY_MALLOC(length * sizeof(jchar));
    if (!chars) {
        throwex_outofmemory(env);
        return NULL;
    }
    jsize size;
    if (!UTF8toUTF16(env, bytes, chars, length, &size)) {
        MEMORY_FREE(chars);
        throwex_msg(env, "Bad UTF-8 coding!");
        return NULL;
    }
    jstring ret = (*env)->NewString(env, chars, size);
    MEMORY_FREE(chars);
    return ret;
}

static const jsize objectLength(JNIEnv *env, jobject object, jint mode) {
    if (mode == ARRAY) {
        return (*env)->GetArrayLength(env, object);
    }
    return (*env)->GetStringLength(env, object) << 2;
}

static const jsize objectToBytes(JNIEnv *env, jobject object, jsize length, char* bytes, jint mode) {
    if (!object) return -1;
    if (mode == ARRAY) {
        (*env)->GetByteArrayRegion(env, object, 0, length, bytes);
        bytes[length] = '\0';
        return length;
    }
    jsize rsize = length >> 2;
#ifdef SQLITE_USE_ALLOCA
    if (rsize < (SQLITE_JDBC_MAX_ALLOCA >> 1)) {
        jchar chars[rsize];
        (*env)->GetStringRegion(env, object, 0, rsize, chars);
        return UTF16toUTF8(env, chars, bytes, length >> 2, &rsize) ? rsize : -1;
    }
#endif
    const jchar* chars = (*env)->GetStringCritical(env, object, NULL);
    jboolean stat = UTF16toUTF8(env, chars, bytes, length >> 2, &rsize);
    (*env)->ReleaseStringCritical(env, object, chars);
    return stat ? rsize : -1;
}

static sqlite3 * gethandle(JNIEnv *env, jobject this)
{
    static jfieldID pointer = 0;
    if (!pointer) pointer = (*env)->GetFieldID(env, dbclass, "pointer", "J");

    return (sqlite3 *)toref((*env)->GetLongField(env, this, pointer));
}

static void sethandle(JNIEnv *env, jobject this, sqlite3 * ref)
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
    static jmethodID exp_msg = 0;
    static jclass exclass = 0;
    if (!exp_msg) {
        exclass = (*env)->FindClass(env, "java/lang/Throwable");
        exp_msg = (*env)->GetMethodID(
                env, exclass, "toString", "()Ljava/lang/String;");
    }

    jthrowable ex = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);

    jstring msg = (jstring)(*env)->CallObjectMethod(env, ex, exp_msg);
    if (!msg) {
        sqlite3_result_error(context, "unknown error", 13); 
        return; 
    }

    jsize length = objectLength(env, msg, STRING);
    char bytes[length + 1];
    length = objectToBytes(env, msg, length, bytes, STRING);
    if (length == -1) {
        sqlite3_result_error_nomem(context); 
        throwex_outofmemory(env);
    } else {
        sqlite3_result_error(context, bytes, length);
    }
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

    dbclass = (*env)->FindClass(env, "org/sqlite/core/NativeDB");
    if (!dbclass) return JNI_ERR;
    dbclass = (*env)->NewGlobalRef(env, dbclass);

    fclass = (*env)->FindClass(env, "org/sqlite/Function");
    if (!fclass) return JNI_ERR;
    fclass = (*env)->NewGlobalRef(env, fclass);

    aclass = (*env)->FindClass(env, "org/sqlite/Function$Aggregate");
    if (!aclass) return JNI_ERR;
    aclass = (*env)->NewGlobalRef(env, aclass);

    wclass = (*env)->FindClass(env, "org/sqlite/Function$Window");
    if (!wclass) return JNI_ERR;
    wclass = (*env)->NewGlobalRef(env, wclass);

    pclass = (*env)->FindClass(env, "org/sqlite/core/DB$ProgressObserver");
    if(!pclass) return JNI_ERR;
    pclass = (*env)->NewGlobalRef(env, pclass);

    phandleclass = (*env)->FindClass(env, "org/sqlite/ProgressHandler");
    if(!phandleclass) return JNI_ERR;
    phandleclass = (*env)->NewGlobalRef(env, phandleclass);

    return JNI_VERSION_1_2;
}

// FINALIZATION

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) 
{
    JNIEnv* env = 0;

    if (JNI_OK != (*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_2))
        return;

    if (dbclass) (*env)->DeleteGlobalRef(env, dbclass);
    if (fclass) (*env)->DeleteGlobalRef(env, fclass);
    if (aclass) (*env)->DeleteGlobalRef(env, aclass);
    if (wclass) (*env)->DeleteGlobalRef(env, wclass);
    if (pclass) (*env)->DeleteGlobalRef(env, pclass);
    if (phandleclass) (*env)->DeleteGlobalRef(env, phandleclass);
}

// WRAPPERS for sqlite_* functions //////////////////////////////////

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_shared_1cache0(
    JNIEnv *env, jobject this, jboolean enable)
{
    return sqlite3_enable_shared_cache(enable ? 1 : 0);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_enable_1load_1extension0(
    JNIEnv *env, jobject this, jboolean enable) 
{
    return sqlite3_enable_load_extension(gethandle(env, this), enable ? 1 : 0);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB__1open0(
    JNIEnv *env, jobject this, jobject file, jint flags, jint mode) 
{
    jsize length = objectLength(env ,file, mode);
    char bytes[length + 1];
    length = objectToBytes(env, file, length, bytes, mode);
    if (length == -1) {
        throwex_outofmemory(env);
        return;
    }

    sqlite3 *db;
    if (sqlite3_open_v2(bytes, &db, flags, NULL) == SQLITE_OK) {
        // Ignore failures, as we can tolerate regular result codes.
        sqlite3_extended_result_codes(db, 1);
        sethandle(env, this, db);
    } else {
        throwex_code(env, sqlite3_extended_errcode(db));
        sqlite3_close(db);
    }
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB__1close0(
    JNIEnv *env, jobject this)
{
    if (sqlite3_close(gethandle(env, this)) != SQLITE_OK) {
        throwex_msg(env, sqlite3_errmsg(gethandle(env, this)));
    }
    sethandle(env, this, 0);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_interrupt0(
    JNIEnv *env, jobject this) 
{
    sqlite3_interrupt(gethandle(env, this));
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_busy_1timeout0(
    JNIEnv *env, jobject this, jint ms)
{   
    sqlite3_busy_timeout(gethandle(env, this), ms);
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

    return (*env)->CallIntMethod(env, busyHandlerContext.obj, busyHandlerContext.methodId, nbPrevInvok);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_busy_1handler0(
    JNIEnv *env, jobject this, jobject busyHandler)
{
    (*env)->GetJavaVM(env, &busyHandlerContext.vm);
    
    if (busyHandler != NULL) {
        busyHandlerContext.obj = (*env)->NewGlobalRef(env, busyHandler);
        busyHandlerContext.methodId = (*env)->GetMethodID(env, 
            (*env)->GetObjectClass(env, busyHandlerContext.obj), 
            "callback", 
            "(I)I");
        sqlite3_busy_handler(gethandle(env, this), &busyHandlerCallBack, NULL);
    } else {
        sqlite3_busy_handler(gethandle(env, this), NULL, NULL);
    }
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_prepare0(
    JNIEnv *env, jobject this, jobject sql, jint mode)
{
    jsize length = objectLength(env, sql, mode);
    char bytes[length + 1];
    length = objectToBytes(env, sql, length, bytes, mode);
    if (length == -1) return fromref(0);

    sqlite3_stmt* stmt;
    int status = sqlite3_prepare_v2(gethandle(env, this), bytes, length, &stmt, 0);
    if (status != SQLITE_OK) {
        throwex_code(env, status);
        return fromref(0);
    }
    return fromref(stmt);
}


JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB__1exec0(
    JNIEnv *env, jobject this, jobject sql, jint mode)
{
    jsize length = objectLength(env, sql, mode);
    char bytes[length + 1];
    length = objectToBytes(env, sql, length, bytes, mode);
    if (length == -1) return SQLITE_ERROR;

    int status = sqlite3_exec(gethandle(env, this), bytes, 0, 0, NULL);
    if (status != SQLITE_OK) {
        throwex_code(env, status);
    }
    return status;
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_errmsg0(
    JNIEnv *env, jobject this, jint mode) 
{
    const char *str = (const char*) sqlite3_errmsg(gethandle(env, this));
    if (!str) return NULL;
    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_libversion0(
    JNIEnv *env, jclass clazz, jint mode)
{
    const char* version = sqlite3_libversion();
    return bytesToObject(env, version, strlen(version), mode);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_changes0(
    JNIEnv *env, jobject this)
{
    return sqlite3_changes(gethandle(env, this));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_total_1changes0(
    JNIEnv *env, jobject this)
{
    return sqlite3_total_changes(gethandle(env, this));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_finalize0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_finalize(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_step0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_step(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_reset0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_reset(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_clear_1bindings0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_clear_bindings(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1parameter_1count0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_bind_parameter_count(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1count0(
    JNIEnv *env, jobject this, jlong stmt)
{
    return sqlite3_column_count(toref(stmt));
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1type0(
    JNIEnv *env, jobject this, jlong stmt, jint col)
{
    return sqlite3_column_type(toref(stmt), col);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1decltype0(
    JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str = (const char*) sqlite3_column_decltype(toref(stmt), col);
    if (!str) return NULL;
    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1table_1name0(
    JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str = sqlite3_column_table_name(toref(stmt), col);
    if (!str) return NULL;
    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1name0(
    JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *str = sqlite3_column_name(toref(stmt), col);
    if (!str) return NULL;
    return bytesToObject(env, str, strlen(str), mode);
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_column_1text0(
    JNIEnv *env, jobject this, jlong stmt, jint col, jint mode)
{
    const char *bytes;
    jsize size;

    bytes = (const char*) sqlite3_column_text(toref(stmt), col);

    if (!bytes && sqlite3_errcode(gethandle(env, this)) == SQLITE_NOMEM) {
        throwex_outofmemory(env);
        return NULL;
    }
    size = sqlite3_column_bytes(toref(stmt), col);
    return bytesToObject(env, bytes, size, mode);
}

JNIEXPORT jbyteArray JNICALL Java_org_sqlite_core_NativeDB_column_1blob0(
    JNIEnv *env, jobject this, jlong stmt, jint col)
{
    int type;
    int length;
    const void *blob;

    // The value returned by sqlite3_column_type() is only meaningful if no type conversions have occurred
    type = sqlite3_column_type(toref(stmt), col);
    blob = sqlite3_column_blob(toref(stmt), col);
    if (!blob && sqlite3_errcode(gethandle(env, this)) == SQLITE_NOMEM) {
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

JNIEXPORT jdouble JNICALL Java_org_sqlite_core_NativeDB_column_1double0(
    JNIEnv *env, jobject this, jlong stmt, jint col)
{
    return sqlite3_column_double(toref(stmt), col);
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_column_1long0(
    JNIEnv *env, jobject this, jlong stmt, jint col)
{
    return sqlite3_column_int64(toref(stmt), col);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_column_1int0(
    JNIEnv *env, jobject this, jlong stmt, jint col)
{
    return sqlite3_column_int(toref(stmt), col);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1null0(
    JNIEnv *env, jobject this, jlong stmt, jint pos)
{
    return sqlite3_bind_null(toref(stmt), pos);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1int0(
    JNIEnv *env, jobject this, jlong stmt, jint pos, jint v)
{
    return sqlite3_bind_int(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1long0(
    JNIEnv *env, jobject this, jlong stmt, jint pos, jlong v)
{
    return sqlite3_bind_int64(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1double0(
    JNIEnv *env, jobject this, jlong stmt, jint pos, jdouble v)
{
    return sqlite3_bind_double(toref(stmt), pos, v);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1text0(
    JNIEnv *env, jobject this, jlong stmt, jint pos, jobject v, jint mode)
{
    if (!v) return sqlite3_bind_null(toref(stmt), pos);

    jsize length = objectLength(env, v, mode);
#ifdef SQLITE_USE_ALLOCA
    if (mode == STRING && length < SQLITE_JDBC_MAX_ALLOCA) {
        char bytes[length + 1];
        length = objectToBytes(env, v, length, bytes, mode);
        return sqlite3_bind_text(toref(stmt), pos, bytes, length, SQLITE_TRANSIENT);
    }
#endif
    char* bytes = MEMORY_MALLOC(length + 1);
    if (!bytes) return SQLITE_ERROR;
    length = objectToBytes(env, v, length, bytes, mode);
    if (length == -1) {
        MEMORY_FREE(bytes);
        return SQLITE_ERROR;
    }
#ifdef SQLITE_JDBC_MEMORY_COMPACT
    bytes = MEMORY_REALLOC(bytes, length);
#endif
    return sqlite3_bind_text(toref(stmt), pos, bytes, length, MEMORY_FREE);
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_bind_1blob0(
    JNIEnv *env, jobject this, jlong stmt, jint pos, jbyteArray v)
{
    if (!v) return sqlite3_bind_null(toref(stmt), pos);

    jsize length = (*env)->GetArrayLength(env, v);
    char* bytes = MEMORY_MALLOC(length);
    if (!bytes) return SQLITE_ERROR;
    (*env)->GetByteArrayRegion(env, v, 0, length, bytes);
    return sqlite3_bind_blob(toref(stmt), pos, bytes, length, MEMORY_FREE);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1null0(
    JNIEnv *env, jobject this, jlong context)
{
    sqlite3_result_null(toref(context));
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1text0(
    JNIEnv *env, jobject this, jlong context, jobject value, jint mode)
{
    if (value == NULL) {
        sqlite3_result_null(toref(context)); 
        return;
    }

    jsize length = objectLength(env, value, mode);
#ifdef SQLITE_USE_ALLOCA
    if (mode == STRING && length < SQLITE_JDBC_MAX_ALLOCA) {
        char bytes[length + 1];
        length = objectToBytes(env, value, length, bytes, mode);
        if (length == -1) {
            sqlite3_result_error_nomem(toref(context));
        } else {
            sqlite3_result_text(toref(context), bytes, length, SQLITE_TRANSIENT);
        }
    }
#endif
    char* bytes = MEMORY_MALLOC(length + 1);
    if (bytes) {
        length = objectToBytes(env, value, length, bytes, mode);
        if (length != -1) {
#ifdef SQLITE_JDBC_MEMORY_COMPACT
            bytes = MEMORY_REALLOC(bytes, length);
#endif
            sqlite3_result_text(toref(context), bytes, length, MEMORY_FREE);
            return;
        }
    }
    sqlite3_result_error_nomem(toref(context));
    if (bytes) MEMORY_FREE(bytes);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1blob0(
    JNIEnv *env, jobject this, jlong context, jbyteArray value)
{
    if (value == NULL) {
        sqlite3_result_null(toref(context)); 
        return;
    }

    jsize length = (*env)->GetArrayLength(env, value);
    char* bytes = MEMORY_MALLOC(length);
    if (bytes) {
        (*env)->GetByteArrayRegion(env, value, 0, length, bytes);
        sqlite3_result_blob(toref(context), bytes, length, MEMORY_FREE);
    } else {
        sqlite3_result_error_nomem(toref(context));
    }
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1double0(
    JNIEnv *env, jobject this, jlong context, jdouble value)
{
    sqlite3_result_double(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1long0(
    JNIEnv *env, jobject this, jlong context, jlong value)
{
    sqlite3_result_int64(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1int0(
    JNIEnv *env, jobject this, jlong context, jint value)
{
    sqlite3_result_int(toref(context), value);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_result_1error0(
    JNIEnv *env, jobject this, jlong context, jobject err, jint mode)
{
    jsize length = objectLength(env, err, mode);
    char bytes[length + 1];
    length = objectToBytes(env, err, length, bytes, mode);
    if (length == -1) {
        sqlite3_result_error_nomem(toref(context));
    } else {
        sqlite3_result_error(toref(context), bytes, length);
    }
}

JNIEXPORT jobject JNICALL Java_org_sqlite_core_NativeDB_value_1text0(
    JNIEnv *env, jobject this, jobject f, jint arg, jint mode)
{
    sqlite3_value *value = tovalue(env, f, arg);
    if (!value) return NULL;

    char* bytes = (char*)sqlite3_value_text(value);
    jsize size = sqlite3_value_bytes(value);
    
    return bytesToObject(env, bytes, size, mode);
}

JNIEXPORT jbyteArray JNICALL Java_org_sqlite_core_NativeDB_value_1blob0(
    JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    if (!value) return NULL;
    const void* blob = sqlite3_value_blob(value);
    if (!blob) return NULL;

    int length = sqlite3_value_bytes(value);
    return bytesToArray(env, (const char*)blob, length);
}

JNIEXPORT jdouble JNICALL Java_org_sqlite_core_NativeDB_value_1double0(
    JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_double(value) : 0;
}

JNIEXPORT jlong JNICALL Java_org_sqlite_core_NativeDB_value_1long0(
    JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_int64(value) : 0;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_value_1int0(
    JNIEnv *env, jobject this, jobject f, jint arg)
{
    sqlite3_value *value = tovalue(env, f, arg);
    return value ? sqlite3_value_int(value) : 0;
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_value_1type0(
    JNIEnv *env, jobject this, jobject func, jint arg)
{
    return sqlite3_value_type(tovalue(env, func, arg));
}


JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_create_1function0(
    JNIEnv *env, jobject this, jobject name, jobject func, jint nArgs, jint flags, jint mode)
{
    int isAgg = 0, isWindow = 0;

    static jfieldID udfdatalist = 0;
    struct UDFData *udf = MEMORY_MALLOC(sizeof(struct UDFData));

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

    jsize length = objectLength(env, name, mode);
    char bytes[length + 1];
    length = objectToBytes(env, name, length, bytes, mode);

    if (isAgg) {
        return sqlite3_create_window_function(
                gethandle(env, this),
                bytes,                 // function name
                nArgs,                 // number of args
                SQLITE_UTF8 | flags,   // preferred chars
                udf,
                &xStep,
                &xFinal,
                isWindow ? &xValue : 0,
                isWindow ? &xInverse : 0,
                0
        );
    } else {
        return sqlite3_create_function(
                gethandle(env, this),
                bytes,                 // function name
                nArgs,                 // number of args
                SQLITE_UTF8 | flags,   // preferred chars
                udf,
                &xFunc,
                0,
                0
        );
    }
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_destroy_1function0(
    JNIEnv *env, jobject this, jobject name, jint nArgs, jint mode)
{
    jsize length = objectLength(env, name, mode);
    char bytes[length + 1];
    length = objectToBytes(env, name, length, bytes, mode);

    return sqlite3_create_function(
        gethandle(env, this), bytes, nArgs, SQLITE_UTF8, 0, 0, 0, 0
    );
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_free_1functions0(
    JNIEnv *env, jobject this)
{
    // clean up all the MEMORY_MALLOC()ed UDFData instances using the
    // linked list stored in DB.udfdatalist
    jfieldID udfdatalist;
    struct UDFData *udf, *udfpass;

    udfdatalist = (*env)->GetFieldID(env, dbclass, "udfdatalist", "J");
    udf = toref((*env)->GetLongField(env, this, udfdatalist));
    (*env)->SetLongField(env, this, udfdatalist, 0);

    while (udf) {
        udfpass = udf->next;
        (*env)->DeleteGlobalRef(env, udf->func);
        MEMORY_FREE(udf);
        udf = udfpass;
    }
}

JNIEXPORT jint JNICALL Java_org_sqlite_core_NativeDB_limit0(
    JNIEnv *env, jobject this, jint id, jint value)
{
    return sqlite3_limit(gethandle(env, this), id, value);
}

// COMPOUND FUNCTIONS ///////////////////////////////////////////////

JNIEXPORT jobjectArray JNICALL Java_org_sqlite_core_NativeDB_column_1metadata0(
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
    dbstmt = toref(stmt);

    colCount = sqlite3_column_count(dbstmt);
    array = (*env)->NewObjectArray(
        env, colCount, (*env)->FindClass(env, "[Z"), NULL) ;
    if (!array) { throwex_outofmemory(env); return 0; }

    colDataRaw = (jboolean*)MEMORY_MALLOC(3 * sizeof(jboolean));
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

    MEMORY_FREE(colDataRaw);

    return array;
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
    
    jsize length = objectLength(env, zFilename, mode);  
    char dFileName[length + 1];
    objectToBytes(env, zFilename, length, dFileName, mode);

    length = objectLength(env, zDBName, mode);
    char dDBName[length + 1];
    objectToBytes(env, zDBName, length, dDBName, mode);

    pDb = gethandle(env, this);

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

    jsize length = objectLength(env, zFilename, mode);
    char dFileName[length + 1];
    length = objectToBytes(env, zFilename, length, dFileName, mode);
    
    length = objectLength(env, zDBName, mode);
    char dDBName[length + 1];
    length = objectToBytes(env, zDBName, length, dDBName, mode);

    int nTimeout = 0;
    pDb = gethandle(env, this);

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

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_register_1progress_1handler0(
  JNIEnv *env, jobject this, jint vmCalls, jobject progressHandler)
{
    progress_handler_context.mth = (*env)->GetMethodID(env, phandleclass, "progress", "()I");
    progress_handler_context.phandler = (*env)->NewGlobalRef(env, progressHandler);
    (*env)->GetJavaVM(env, &progress_handler_context.vm);
    sqlite3_progress_handler(gethandle(env, this), vmCalls, &progress_handler_function, NULL);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_clear_1progress_1handler0(
  JNIEnv *env, jobject this)
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


void update_hook(void *context, int type, char const *database, char const *table, sqlite3_int64 row) 
{
    JNIEnv *env = 0;
    (*update_handler_context.vm)->AttachCurrentThread(update_handler_context.vm, (void **)&env, 0);

    jstring tableString = bytesToObject(env, table, strlen(table), STRING);
    jstring databaseString = bytesToObject(env, database, strlen(database), STRING);
    (*env)->CallVoidMethod(env, update_handler_context.handler, update_handler_context.method, type, databaseString, tableString, row);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_set_1update_1listener0(
    JNIEnv *env, jobject this, jboolean enabled)
{
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

int commit_hook(void *context) 
{
    JNIEnv *env = 0;
    (*commit_handler_context.vm)->AttachCurrentThread(commit_handler_context.vm, (void **)&env, 0);
    (*env)->CallVoidMethod(env, commit_handler_context.handler, commit_handler_context.method, 1);
    return 0;
}

void rollback_hook(void *context) 
{
    JNIEnv *env = 0;
    (*commit_handler_context.vm)->AttachCurrentThread(commit_handler_context.vm, (void **)&env, 0);
    (*env)->CallVoidMethod(env, commit_handler_context.handler, commit_handler_context.method, 0);
}

JNIEXPORT void JNICALL Java_org_sqlite_core_NativeDB_set_1commit_1listener0(
    JNIEnv *env, jobject this, jboolean enabled) 
{
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