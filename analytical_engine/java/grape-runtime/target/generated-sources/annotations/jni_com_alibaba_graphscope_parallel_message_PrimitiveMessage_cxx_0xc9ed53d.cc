#include <jni.h>
#include <new>
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_message_PrimitiveMessage_1cxx_10xc9ed53d__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::PrimitiveMessage<double>);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_parallel_message_PrimitiveMessage_1cxx_10xc9ed53d_nativeGetData(JNIEnv*, jclass, jlong ptr) {
	return (jdouble)(reinterpret_cast<gs::PrimitiveMessage<double>*>(ptr)->getData());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_message_PrimitiveMessage_1cxx_10xc9ed53d_nativeSetData(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* arg00 */) {
	reinterpret_cast<gs::PrimitiveMessage<double>*>(ptr)->setData(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_PrimitiveMessage_1cxx_10xc9ed53d_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::PrimitiveMessage<double>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_PrimitiveMessage_1cxx_10xc9ed53d_nativeCreateFactory1(JNIEnv*, jclass, jdouble arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new gs::PrimitiveMessage<double>(arg0));
}

#ifdef __cplusplus
}
#endif
