#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Nbr<uint64_t,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeCopy(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Nbr<uint64_t,int64_t>(*reinterpret_cast<grape::Nbr<uint64_t,int64_t>*>(ptr)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::Nbr<uint64_t,int64_t>*>(ptr)->data);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeNeighbor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<grape::Nbr<uint64_t,int64_t>*>(ptr)->neighbor));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,int64_t>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,int64_t>(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10x979f8e12_nativeCreateFactory2(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,int64_t>(arg0, arg1));
}

#ifdef __cplusplus
}
#endif
