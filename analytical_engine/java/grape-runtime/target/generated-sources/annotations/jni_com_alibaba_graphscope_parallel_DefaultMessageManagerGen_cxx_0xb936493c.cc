#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/java_messages.h"
#include "grape/graph/adj_list.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::DefaultMessageManager);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeFinishARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->FinishARound();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeForceContinue(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->ForceContinue();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeGetMsgSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::DefaultMessageManager*>(ptr)->GetMsgSize());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeGetPureMessage0(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<grape::DefaultMessageManager*>(ptr)->GetMessage<std::vector<char>>(*reinterpret_cast<std::vector<char>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeSendToFragment0(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->SendToFragment<std::vector<char>>(arg0, *reinterpret_cast<std::vector<char>*>(arg1));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeStart(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->Start();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeStartARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->StartARound();
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_nativeToTerminate(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<grape::DefaultMessageManager*>(ptr)->ToTerminate()) ? JNI_TRUE : JNI_FALSE;
}

#ifdef __cplusplus
}
#endif
