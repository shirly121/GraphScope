#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowArrayBuilder<double>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0_nativeGetValue(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)(reinterpret_cast<gs::ArrowArrayBuilder<double>*>(ptr)->GetValue(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowArrayBuilder<double>*>(ptr)->Reserve(arg0)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	(*reinterpret_cast<gs::ArrowArrayBuilder<double>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0_nativeUnsafeAppend(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* arg00 */) {
	reinterpret_cast<gs::ArrowArrayBuilder<double>*>(ptr)->UnsafeAppend(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x232959b0_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowArrayBuilder<double>());
}

#ifdef __cplusplus
}
#endif
