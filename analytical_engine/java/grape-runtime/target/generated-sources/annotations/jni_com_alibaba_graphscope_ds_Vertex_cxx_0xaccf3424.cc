#include <jni.h>
#include <new>
#include "grape/utils/vertex_array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Vertex<uint64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeCopy(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Vertex<uint64_t>(*reinterpret_cast<grape::Vertex<uint64_t>*>(ptr)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::Vertex<uint64_t>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeEq(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return ((*reinterpret_cast<grape::Vertex<uint64_t>*>(ptr)) == (*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeGetValue(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::Vertex<uint64_t>*>(ptr)->GetValue());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeInc(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(++(*reinterpret_cast<grape::Vertex<uint64_t>*>(ptr))));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeSetValue(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<grape::Vertex<uint64_t>*>(ptr)->SetValue(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Vertex_1cxx_10xaccf3424_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::Vertex<uint64_t>());
}

#ifdef __cplusplus
}
#endif
