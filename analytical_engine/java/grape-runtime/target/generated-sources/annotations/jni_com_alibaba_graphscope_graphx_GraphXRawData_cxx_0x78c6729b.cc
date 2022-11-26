#include <jni.h>
#include <new>
#include "core/java/graphx_raw_data.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x78c6729b__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x78c6729b_nativeGetEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>*>(ptr)->GetEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x78c6729b_nativeGetVertexNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>*>(ptr)->GetVertexNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawData_1cxx_10x78c6729b_nativeId(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>*>(ptr)->id());
}

#ifdef __cplusplus
}
#endif
