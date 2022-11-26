#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"
#include "grape/types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_GrapeAdjList_1cxx_10x8c030f50__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::AdjList<uint64_t,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeAdjList_1cxx_10x8c030f50_nativeBegin_1pointer(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<grape::AdjList<uint64_t,double>*>(ptr)->begin_pointer());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_GrapeAdjList_1cxx_10x8c030f50_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::AdjList<uint64_t,double>*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeAdjList_1cxx_10x8c030f50_nativeEnd_1pointer(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<grape::AdjList<uint64_t,double>*>(ptr)->end_pointer());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeAdjList_1cxx_10x8c030f50_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::AdjList<uint64_t,double>*>(ptr)->Size());
}

#ifdef __cplusplus
}
#endif
