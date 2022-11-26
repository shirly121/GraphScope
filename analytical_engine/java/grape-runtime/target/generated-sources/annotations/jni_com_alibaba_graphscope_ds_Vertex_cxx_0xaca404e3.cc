#include <jni.h>
#include <new>
#include "grape/utils/vertex_array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Vertex<uint32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeCopy(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Vertex<uint32_t>(*reinterpret_cast<grape::Vertex<uint32_t>*>(ptr)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::Vertex<uint32_t>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeEq(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return ((*reinterpret_cast<grape::Vertex<uint32_t>*>(ptr)) == (*reinterpret_cast<grape::Vertex<uint32_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeGetValue(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<grape::Vertex<uint32_t>*>(ptr)->GetValue());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeInc(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(++(*reinterpret_cast<grape::Vertex<uint32_t>*>(ptr))));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeSetValue(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	reinterpret_cast<grape::Vertex<uint32_t>*>(ptr)->SetValue(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaca404e3_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::Vertex<uint32_t>());
}

#ifdef __cplusplus
}
#endif
