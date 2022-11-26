#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowArrayBuilder<int32_t>);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8_nativeGetValue(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowArrayBuilder<int32_t>*>(ptr)->GetValue(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowArrayBuilder<int32_t>*>(ptr)->Reserve(arg0)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	(*reinterpret_cast<gs::ArrowArrayBuilder<int32_t>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8_nativeUnsafeAppend(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	reinterpret_cast<gs::ArrowArrayBuilder<int32_t>*>(ptr)->UnsafeAppend(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_PrimitiveArrowArrayBuilder_1cxx_10x101bead8_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowArrayBuilder<int32_t>());
}

#ifdef __cplusplus
}
#endif
