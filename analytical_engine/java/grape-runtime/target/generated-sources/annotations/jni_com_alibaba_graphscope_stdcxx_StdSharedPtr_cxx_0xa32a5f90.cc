#include <jni.h>
#include <new>
#include <memory>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdSharedPtr_1cxx_10xa32a5f90__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdSharedPtr_1cxx_10xa32a5f90_nativeGet(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>>*>(ptr)->get());
}

#ifdef __cplusplus
}
#endif
