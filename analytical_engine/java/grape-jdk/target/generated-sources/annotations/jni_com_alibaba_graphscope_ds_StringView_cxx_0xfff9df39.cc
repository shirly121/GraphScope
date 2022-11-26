#include <jni.h>
#include <new>
#include "vineyard/basic/ds/arrow_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10xfff9df39__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::arrow_string_view);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10xfff9df39_nativeByteAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */) {
	return (jbyte)((*reinterpret_cast<vineyard::arrow_string_view*>(ptr))[arg0]);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10xfff9df39_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<vineyard::arrow_string_view*>(ptr)->data());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10xfff9df39_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<vineyard::arrow_string_view*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringView_1cxx_10xfff9df39_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<vineyard::arrow_string_view*>(ptr)->size());
}

#ifdef __cplusplus
}
#endif
