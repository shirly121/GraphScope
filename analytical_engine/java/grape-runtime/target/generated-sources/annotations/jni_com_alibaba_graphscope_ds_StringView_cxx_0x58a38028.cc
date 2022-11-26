#include <jni.h>
#include <new>
#include "arrow/util/string_view.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10x58a38028__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(arrow::util::string_view);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10x58a38028_nativeByteAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jbyte)((*reinterpret_cast<arrow::util::string_view*>(ptr))[arg0]);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10x58a38028_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<arrow::util::string_view*>(ptr)->data());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10x58a38028_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<arrow::util::string_view*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10x58a38028_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<arrow::util::string_view*>(ptr)->size());
}

#ifdef __cplusplus
}
#endif
