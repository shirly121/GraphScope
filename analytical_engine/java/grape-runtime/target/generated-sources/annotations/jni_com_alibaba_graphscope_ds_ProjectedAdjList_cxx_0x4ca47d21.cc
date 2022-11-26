#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21_nativeBegin(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int64_t>(reinterpret_cast<gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>*>(ptr)->begin()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>*>(ptr)->Empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21_nativeEnd(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int64_t>(reinterpret_cast<gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>*>(ptr)->end()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21_nativeNotEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>*>(ptr)->NotEmpty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedAdjList_1cxx_10x4ca47d21_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>*>(ptr)->Size());
}

#ifdef __cplusplus
}
#endif
