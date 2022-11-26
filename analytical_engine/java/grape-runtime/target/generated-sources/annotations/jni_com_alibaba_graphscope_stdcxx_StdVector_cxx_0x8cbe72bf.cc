#include <jni.h>
#include <new>
#include <vector>
#include <string>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<gs::VertexArrayDefault<double>>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(&((*reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr))[arg0]));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativePush_1back(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->push_back(*reinterpret_cast<gs::VertexArrayDefault<double>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	(*reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr))[arg0] = *reinterpret_cast<gs::VertexArrayDefault<double>*>(arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<gs::VertexArrayDefault<double>>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10x8cbe72bf_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<gs::VertexArrayDefault<double>>());
}

#ifdef __cplusplus
}
#endif
