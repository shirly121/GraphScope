#include <jni.h>
#include <new>
#include "grape/app/vertex_data_context.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_context_ffi_FFIVertexDataContext_1cxx_10xc999cdfb__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::VertexDataContext<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>,int32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_context_ffi_FFIVertexDataContext_1cxx_10xc999cdfb_nativeData(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<grape::VertexDataContext<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>,int32_t>*>(ptr)->data()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_context_ffi_FFIVertexDataContext_1cxx_10xc999cdfb_nativeCreateFactory0(JNIEnv*, jclass, jlong arg0 /* arg00 */, jboolean arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new grape::VertexDataContext<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>,int32_t>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>*>(arg0), (arg1 == JNI_TRUE ? true : false)));
}

#ifdef __cplusplus
}
#endif
