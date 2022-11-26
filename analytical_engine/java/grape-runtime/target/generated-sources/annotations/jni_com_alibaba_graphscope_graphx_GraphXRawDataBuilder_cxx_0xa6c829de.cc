#include <jni.h>
#include <new>
#include "core/java/graphx_raw_data.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawDataBuilder_1cxx_10xa6c829de__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::GraphXRawDataBuilder<int64_t,uint64_t,int64_t,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawDataBuilder_1cxx_10xa6c829de_nativeSeal(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>>(reinterpret_cast<gs::GraphXRawDataBuilder<int64_t,uint64_t,int64_t,int64_t>*>(ptr)->MySeal(*reinterpret_cast<vineyard::Client*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_GraphXRawDataBuilder_1cxx_10xa6c829de_nativeCreateFactory0(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */, jlong arg3 /* arg33 */, jlong arg4 /* arg44 */, jlong arg5 /* arg55 */) {
	return reinterpret_cast<jlong>(new gs::GraphXRawDataBuilder<int64_t,uint64_t,int64_t,int64_t>(*reinterpret_cast<vineyard::Client*>(arg0), *reinterpret_cast<std::vector<int64_t>*>(arg1), *reinterpret_cast<std::vector<int64_t>*>(arg2), *reinterpret_cast<std::vector<int64_t>*>(arg3), *reinterpret_cast<std::vector<int64_t>*>(arg4), *reinterpret_cast<std::vector<int64_t>*>(arg5)));
}

#ifdef __cplusplus
}
#endif
