#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x5d3f85f1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<std::string>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x5d3f85f1_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<std::string>*>(ptr)->GetLength());
}

#ifdef __cplusplus
}
#endif
