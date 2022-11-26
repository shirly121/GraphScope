#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x708692cf__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::TypedArray<arrow::util::string_view>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_BaseTypedArray_1cxx_10x708692cf_nativeGetLength(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::TypedArray<arrow::util::string_view>*>(ptr)->GetLength());
}

#ifdef __cplusplus
}
#endif
