#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::JavaVertexArray<uint64_t,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_native_1GetVertexRange(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->GetVertexRange()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr))[*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeInit0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeInit1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->Init(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeSetValue0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->SetValue(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeSetValue1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->SetValue(*reinterpret_cast<grape::VertexRange<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeSetValue2(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* arg00 */) {
	reinterpret_cast<gs::JavaVertexArray<uint64_t,double>*>(ptr)->SetValue(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_VertexArray_1cxx_10xbe45efc3_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::JavaVertexArray<uint64_t,double>());
}

#ifdef __cplusplus
}
#endif
