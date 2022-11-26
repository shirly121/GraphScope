#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PropertyNbrUnit_1cxx_10xe6668a20__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::NbrUnitDefault<uint64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyNbrUnit_1cxx_10xe6668a20_nativeEid(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::NbrUnitDefault<uint64_t>*>(ptr)->eid);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyNbrUnit_1cxx_10xe6668a20_nativeGetNeighbor(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Vertex<uint64_t>(reinterpret_cast<gs::NbrUnitDefault<uint64_t>*>(ptr)->get_neighbor()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyNbrUnit_1cxx_10xe6668a20_nativeVid(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::NbrUnitDefault<uint64_t>*>(ptr)->vid);
}

#ifdef __cplusplus
}
#endif
