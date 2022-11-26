#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x2d753886__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x2d753886_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<double>*>(ptr)->GetLength());
}

#ifdef __cplusplus
}
#endif
