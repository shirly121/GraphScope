#include <string>
#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include <cstdint>
#include "grape/types.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983_nativeAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)(reinterpret_cast<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->at(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983_nativeData(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->data()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983_nativeName(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->name()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	reinterpret_cast<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->set(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_column_DoubleColumn_1cxx_10x26811983_nativeSetName(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>*>(ptr)->set_name(*reinterpret_cast<std::string*>(arg0));
}

#ifdef __cplusplus
}
#endif
