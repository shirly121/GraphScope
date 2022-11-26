#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::RawAdjListDefault<uint64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad_nativeBegin(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::RawAdjListDefault<uint64_t>*>(ptr)->begin());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<gs::RawAdjListDefault<uint64_t>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<gs::RawAdjListDefault<uint64_t>*>(ptr)->Empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad_nativeEnd(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::RawAdjListDefault<uint64_t>*>(ptr)->end());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_PropertyRawAdjList_1cxx_10xae65a4ad_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::RawAdjListDefault<uint64_t>*>(ptr)->Size());
}

#ifdef __cplusplus
}
#endif
