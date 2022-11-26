#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include <cstdint>
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<std::string>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1_nativeGet(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::util::string_view((*reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<std::string>*>(ptr))[arg0]));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<std::string>*>(ptr)->GetLength());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1_nativeGetRawData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<std::string>*>(ptr)->GetRawData());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1_nativeGetRawDataLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<std::string>*>(ptr)->GetRawDataLength());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_StringTypedArray_1cxx_10x5d3f85f1_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::arrow_projected_fragment_impl::TypedArray<std::string>());
}

#ifdef __cplusplus
}
#endif
