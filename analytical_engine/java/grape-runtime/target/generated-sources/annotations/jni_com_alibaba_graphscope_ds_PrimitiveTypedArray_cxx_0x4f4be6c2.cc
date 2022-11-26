#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x4f4be6c2__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<int32_t>);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x4f4be6c2_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)((*reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<int32_t>*>(ptr))[arg0]);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x4f4be6c2_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<int32_t>*>(ptr)->GetLength());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x4f4be6c2_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::arrow_projected_fragment_impl::TypedArray<int32_t>());
}

#ifdef __cplusplus
}
#endif
