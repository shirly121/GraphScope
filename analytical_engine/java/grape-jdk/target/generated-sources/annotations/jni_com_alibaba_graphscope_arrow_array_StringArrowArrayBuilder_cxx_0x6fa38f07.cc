#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArrayBuilder_1cxx_10x6fa38f07__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowArrayBuilder<std::string>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArrayBuilder_1cxx_10x6fa38f07_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* additionalCapacity0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowArrayBuilder<std::string>*>(ptr)->Reserve(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArrayBuilder_1cxx_10x6fa38f07_nativeReserveData(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* additionalBytes0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowArrayBuilder<std::string>*>(ptr)->ReserveData(arg0)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArrayBuilder_1cxx_10x6fa38f07_nativeUnsafeAppend(JNIEnv*, jclass, jlong ptr, jlong arg0 /* ptr0 */, jint arg1 /* length1 */) {
	reinterpret_cast<gs::ArrowArrayBuilder<std::string>*>(ptr)->UnsafeAppend(reinterpret_cast<const char*>(arg0), arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArrayBuilder_1cxx_10x6fa38f07_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowArrayBuilder<std::string>());
}

#ifdef __cplusplus
}
#endif
