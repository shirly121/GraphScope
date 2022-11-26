#include <jni.h>
#include <new>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/fragment_getter.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10x209d97e8__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,double,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10x209d97e8_nativeGet(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>(reinterpret_cast<gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,double,int64_t>*>(ptr)->Get(*reinterpret_cast<vineyard::Client*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10x209d97e8_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,double,int64_t>());
}

#ifdef __cplusplus
}
#endif
