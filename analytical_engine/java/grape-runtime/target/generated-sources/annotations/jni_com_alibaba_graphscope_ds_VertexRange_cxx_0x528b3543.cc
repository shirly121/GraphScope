#include <jni.h>
#include <new>
#include "grape/utils/vertex_array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::VertexRange<uint64_t>);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_native_1SetRange(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	reinterpret_cast<grape::VertexRange<uint64_t>*>(ptr)->SetRange(arg0, arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeBeginValue(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::VertexRange<uint64_t>*>(ptr)->begin_value());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::VertexRange<uint64_t>*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeEndValue(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::VertexRange<uint64_t>*>(ptr)->end_value());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::VertexRange<uint64_t>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::VertexRange<uint64_t>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexRange_1cxx_10x528b3543_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new grape::VertexRange<uint64_t>(arg0, arg1));
}

#ifdef __cplusplus
}
#endif
