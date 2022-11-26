#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include <cstdint>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "grape/types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_column_IntColumn_1cxx_10x66bea13d__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_column_IntColumn_1cxx_10x66bea13d_nativeAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)(reinterpret_cast<gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->at(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_column_IntColumn_1cxx_10x66bea13d_nativeData(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->data()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_column_IntColumn_1cxx_10x66bea13d_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	reinterpret_cast<gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->set(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

#ifdef __cplusplus
}
#endif
