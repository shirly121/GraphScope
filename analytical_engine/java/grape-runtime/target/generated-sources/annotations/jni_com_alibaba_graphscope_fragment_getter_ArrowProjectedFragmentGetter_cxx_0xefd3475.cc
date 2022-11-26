#include <jni.h>
#include <new>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/fragment_getter.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xefd3475__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,int32_t,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xefd3475_nativeGet(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,double>>(reinterpret_cast<gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,int32_t,double>*>(ptr)->Get(*reinterpret_cast<vineyard::Client*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xefd3475_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,int32_t,double>());
}

#ifdef __cplusplus
}
#endif
