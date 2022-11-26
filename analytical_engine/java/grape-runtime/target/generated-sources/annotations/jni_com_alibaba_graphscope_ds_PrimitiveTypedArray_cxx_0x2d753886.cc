#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x2d753886__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<double>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x2d753886_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<double>*>(ptr))[arg0]);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x2d753886_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<double>*>(ptr)->GetLength());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PrimitiveTypedArray_1cxx_10x2d753886_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::arrow_projected_fragment_impl::TypedArray<double>());
}

#ifdef __cplusplus
}
#endif
