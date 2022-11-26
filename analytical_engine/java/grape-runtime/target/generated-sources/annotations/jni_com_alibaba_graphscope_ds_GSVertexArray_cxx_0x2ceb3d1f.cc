#include <jni.h>
#include <new>
#include "core/config.h"
#include "core/java/type_alias.h"
#include "grape/utils/vertex_array.h"
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::VertexArrayDefault<int32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_native_1GetVertexRange(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->GetVertexRange()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)((*reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr))[*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeInit0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeInit1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeSetValue0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->SetValue(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeSetValue1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->SetValue(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeSetValue2(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->SetValue(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->size());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeSwap(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(ptr)->Swap(*reinterpret_cast<gs::VertexArrayDefault<int32_t>*>(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x2ceb3d1f_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::VertexArrayDefault<int32_t>());
}

#ifdef __cplusplus
}
#endif
