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
jint JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::VertexArrayDefault<double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_native_1GetVertexRange(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->GetVertexRange()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr))[*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeInit0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeInit1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeSetValue0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->SetValue(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeSetValue1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->SetValue(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeSetValue2(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->SetValue(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->size());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeSwap(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::VertexArrayDefault<double>*>(ptr)->Swap(*reinterpret_cast<gs::VertexArrayDefault<double>*>(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GSVertexArray_1cxx_10x13932289_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::VertexArrayDefault<double>());
}

#ifdef __cplusplus
}
#endif
