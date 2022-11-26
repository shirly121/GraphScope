#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7_nativeDec(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(--(*reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(ptr))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7_nativeEdgeId(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(ptr)->edge_id());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7_nativeEq(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return ((*reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(ptr)) == (*reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7_nativeInc(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(++(*reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(ptr))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_ProjectedNbr_1cxx_10xd37d5fa7_nativeNeighbor(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::Vertex<uint64_t>(reinterpret_cast<gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>*>(ptr)->neighbor()));
}

#ifdef __cplusplus
}
#endif
