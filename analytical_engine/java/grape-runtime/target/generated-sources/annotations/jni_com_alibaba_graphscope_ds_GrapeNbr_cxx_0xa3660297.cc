#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Nbr<uint64_t,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeCopy(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Nbr<uint64_t,double>(*reinterpret_cast<grape::Nbr<uint64_t,double>*>(ptr)));
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jdouble)(reinterpret_cast<grape::Nbr<uint64_t,double>*>(ptr)->data);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeNeighbor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<grape::Nbr<uint64_t,double>*>(ptr)->neighbor));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,double>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,double>(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_GrapeNbr_1cxx_10xa3660297_nativeCreateFactory2(JNIEnv*, jclass, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new grape::Nbr<uint64_t,double>(arg0, arg1));
}

#ifdef __cplusplus
}
#endif
