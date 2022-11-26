#include <jni.h>
#include <new>
#include "vineyard/basic/ds/array.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::ArrayBuilder<double>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<vineyard::ArrayBuilder<double>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	(*reinterpret_cast<vineyard::ArrayBuilder<double>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<vineyard::ArrayBuilder<double>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17_nativeCreateFactory0(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new vineyard::ArrayBuilder<double>(*reinterpret_cast<vineyard::Client*>(arg0), *reinterpret_cast<std::vector<double>*>(arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardArrayBuilder_1cxx_10xc3957a17_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new vineyard::ArrayBuilder<double>(*reinterpret_cast<vineyard::Client*>(arg0), arg1));
}

#ifdef __cplusplus
}
#endif
