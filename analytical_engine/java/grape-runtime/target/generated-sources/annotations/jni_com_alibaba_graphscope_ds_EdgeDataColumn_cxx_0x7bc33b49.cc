#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_EdgeDataColumn_1cxx_10x7bc33b49__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::EdgeDataColumnDefault<double>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_ds_EdgeDataColumn_1cxx_10x7bc33b49_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<gs::EdgeDataColumnDefault<double>*>(ptr))[*reinterpret_cast<const gs::NbrUnitDefault<uint64_t>*>(arg0)]);
}

#ifdef __cplusplus
}
#endif
