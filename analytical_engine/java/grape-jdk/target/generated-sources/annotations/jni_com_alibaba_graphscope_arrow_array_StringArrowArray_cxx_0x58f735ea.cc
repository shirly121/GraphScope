#include <string>
#include <jni.h>
#include <new>
#include "core/java/type_alias.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArray_1cxx_10x58f735ea__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowArray<std::string>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArray_1cxx_10x58f735ea_nativeGetString(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* index0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<gs::ArrowArray<std::string>*>(ptr)->GetString(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_StringArrowArray_1cxx_10x58f735ea_nativeGetView(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* index0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::util::string_view(reinterpret_cast<gs::ArrowArray<std::string>*>(ptr)->GetView(arg0)));
}

#ifdef __cplusplus
}
#endif
