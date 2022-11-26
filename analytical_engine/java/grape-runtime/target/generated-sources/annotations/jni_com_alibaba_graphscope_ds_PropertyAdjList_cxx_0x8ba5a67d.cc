#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::AdjListDefault<uint64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeBegin(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::NbrDefault<uint64_t>(reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->begin()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeBeginUnit(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->begin_unit());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->Empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeEnd(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::NbrDefault<uint64_t>(reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->end()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeEndUnit(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->end_unit());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PropertyAdjList_1cxx_10x8ba5a67d_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::AdjListDefault<uint64_t>*>(ptr)->Size());
}

#ifdef __cplusplus
}
#endif
