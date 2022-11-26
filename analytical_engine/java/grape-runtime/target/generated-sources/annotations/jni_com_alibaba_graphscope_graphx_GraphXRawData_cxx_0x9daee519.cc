#include <jni.h>
#include <new>
#include "core/java/graphx_raw_data.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x9daee519__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x9daee519_nativeGetEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>*>(ptr)->GetEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x9daee519_nativeGetVertexNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>*>(ptr)->GetVertexNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x9daee519_nativeId(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>*>(ptr)->id());
}

#ifdef __cplusplus
}
#endif
