#include <jni.h>
#include <new>
#include "grape/parallel/parallel_message_manager.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/graph/adj_list.h"
#include "core/java/java_messages.h"
#include "grape/parallel/parallel_message_manager.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/graph/adj_list.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::ParallelMessageManager);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeFinishARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->FinishARound();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeForceContinue(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->ForceContinue();
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeGetMessageInBuffer(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<grape::ParallelMessageManager*>(ptr)->GetMessageInBuffer(*reinterpret_cast<grape::MessageInBuffer*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeGetMsgSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::ParallelMessageManager*>(ptr)->GetMsgSize());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeInitChannels(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->InitChannels(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendMsgThroughIEdgesArrowProjected0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jdouble arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendMsgThroughIEdges<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>,double>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int32_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), ((double) arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendMsgThroughIEdgesArrowProjected1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jdouble arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendMsgThroughIEdges<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>,double>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), ((double) arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendMsgThroughIEdgesArrowProjected2(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jdouble arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendMsgThroughIEdges<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>,double>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), ((double) arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendMsgThroughOEdgesArrowProjected0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendMsgThroughOEdges<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>,gs::DoubleMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::DoubleMsg*>(arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendMsgThroughOEdgesArrowProjected1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendMsgThroughOEdges<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>,gs::DoubleMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::DoubleMsg*>(arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSendToFragment0(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */, jint arg2 /* arg22 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SendToFragment<std::vector<char>>(arg0, *reinterpret_cast<std::vector<char>*>(arg1), arg2);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeStart(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->Start();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeStartARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->StartARound();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSyncStateOnOuterVertexArrowProjected0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SyncStateOnOuterVertex<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>,gs::DoubleMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::DoubleMsg*>(arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSyncStateOnOuterVertexArrowProjected1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */, jint arg3 /* arg33 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SyncStateOnOuterVertex<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>,gs::LongMsg>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), *reinterpret_cast<gs::LongMsg*>(arg2), arg3);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSyncStateOnOuterVertexArrowProjectedNoMsg0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jint arg2 /* arg22 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SyncStateOnOuterVertex<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), arg2);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeSyncStateOnOuterVertexArrowProjectedNoMsg1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */, jint arg2 /* arg22 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->SyncStateOnOuterVertex<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>(*reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>*>(arg0), *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1), arg2);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeToTerminate(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<grape::ParallelMessageManager*>(ptr)->ToTerminate()) ? JNI_TRUE : JNI_FALSE;
}

#ifdef __cplusplus
}
#endif
