#include <jni.h>
#include <new>
#include "grape/utils/vertex_array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::VertexRange<uint32_t>);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_native_1SetRange(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jint arg1 /* arg11 */) {
	reinterpret_cast<grape::VertexRange<uint32_t>*>(ptr)->SetRange(arg0, arg1);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeBeginValue(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<grape::VertexRange<uint32_t>*>(ptr)->begin_value());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::VertexRange<uint32_t>*>(ptr);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeEndValue(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<grape::VertexRange<uint32_t>*>(ptr)->end_value());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::VertexRange<uint32_t>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::VertexRange<uint32_t>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x52600602_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new grape::VertexRange<uint32_t>(arg0, arg1));
}

#ifdef __cplusplus
}
#endif
