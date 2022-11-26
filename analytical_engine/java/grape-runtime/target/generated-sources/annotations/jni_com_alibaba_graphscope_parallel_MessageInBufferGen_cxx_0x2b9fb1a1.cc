#include <jni.h>
#include <new>
#include "grape/parallel/message_in_buffer.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"
#include "core/java/java_messages.h"
#include "grape/parallel/message_in_buffer.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::MessageInBuffer);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeGetMessageArrowProjected0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<grape::MessageInBuffer*>(ptr)->GetMessage<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>,grape::EmptyType>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<grape::EmptyType*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeGetMessageArrowProjected1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<grape::MessageInBuffer*>(ptr)->GetMessage<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>,gs::DoubleMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::DoubleMsg*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeGetMessageArrowProjected2(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<grape::MessageInBuffer*>(ptr)->GetMessage<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>,gs::LongMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::LongMsg*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeGetPureMessage0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<grape::MessageInBuffer*>(ptr)->GetMessage<std::vector<char>>(*reinterpret_cast<std::vector<char>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::MessageInBuffer());
}

#ifdef __cplusplus
}
#endif
