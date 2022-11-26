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
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdSharedPtr_1cxx_10xea22d113__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,std::string,std::string>>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdSharedPtr_1cxx_10xea22d113_nativeGet(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,std::string,std::string>>*>(ptr)->get());
}

#ifdef __cplusplus
}
#endif
