#include <jni.h>
#include <new>
#include "core/context/vertex_data_context.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_context_ffi_FFILabeledVertexDataContext_1cxx_10xc64d9ac3__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::LabeledVertexDataContext<gs::ArrowFragmentDefault<int64_t>,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_context_ffi_FFILabeledVertexDataContext_1cxx_10xc64d9ac3_nativeData(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::LabeledVertexDataContext<gs::ArrowFragmentDefault<int64_t>,double>*>(ptr)->data()));
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_context_ffi_FFILabeledVertexDataContext_1cxx_10xc64d9ac3_nativeGetValue(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)(reinterpret_cast<gs::LabeledVertexDataContext<gs::ArrowFragmentDefault<int64_t>,double>*>(ptr)->GetValue(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_context_ffi_FFILabeledVertexDataContext_1cxx_10xc64d9ac3_nativeCreateFactory0(JNIEnv*, jclass, jlong arg0 /* arg00 */, jboolean arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new gs::LabeledVertexDataContext<gs::ArrowFragmentDefault<int64_t>,double>(*reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(arg0), (arg1 == JNI_TRUE ? true : false)));
}

#ifdef __cplusplus
}
#endif
