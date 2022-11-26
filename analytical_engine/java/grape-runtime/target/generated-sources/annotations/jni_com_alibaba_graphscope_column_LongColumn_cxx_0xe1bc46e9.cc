#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include <cstdint>
#include "grape/types.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_column_LongColumn_1cxx_10xe1bc46e9__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_column_LongColumn_1cxx_10xe1bc46e9_nativeAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)(reinterpret_cast<gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>*>(ptr)->at(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_column_LongColumn_1cxx_10xe1bc46e9_nativeData(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>*>(ptr)->data()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_column_LongColumn_1cxx_10xe1bc46e9_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	reinterpret_cast<gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>*>(ptr)->set(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1);
}

#ifdef __cplusplus
}
#endif
