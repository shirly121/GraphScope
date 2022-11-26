#include <jni.h>
#include <new>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_FidPointer_1cxx_10xff0c66f5__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(unsigned);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_FidPointer_1cxx_10xff0c66f5_nativeGet(JNIEnv*, jclass, jlong ptr) {
	return (jint)(*reinterpret_cast<unsigned*>(ptr));
}

#ifdef __cplusplus
}
#endif
