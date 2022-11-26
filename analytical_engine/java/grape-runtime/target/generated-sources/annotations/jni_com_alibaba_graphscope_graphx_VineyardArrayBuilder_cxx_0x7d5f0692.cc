#include <jni.h>
#include <new>
#include "vineyard/basic/ds/array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::ArrayBuilder<int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)((*reinterpret_cast<vineyard::ArrayBuilder<int64_t>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	(*reinterpret_cast<vineyard::ArrayBuilder<int64_t>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<vineyard::ArrayBuilder<int64_t>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692_nativeCreateFactory0(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new vineyard::ArrayBuilder<int64_t>(*reinterpret_cast<vineyard::Client*>(arg0), *reinterpret_cast<std::vector<int64_t>*>(arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10x7d5f0692_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new vineyard::ArrayBuilder<int64_t>(*reinterpret_cast<vineyard::Client*>(arg0), arg1));
}

#ifdef __cplusplus
}
#endif
