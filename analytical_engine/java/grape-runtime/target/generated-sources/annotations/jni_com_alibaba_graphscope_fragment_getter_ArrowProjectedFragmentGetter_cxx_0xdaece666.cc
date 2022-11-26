#include <jni.h>
#include <new>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/fragment_getter.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xdaece666__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,std::string,double>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xdaece666_nativeGet(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,std::string,double>>(reinterpret_cast<gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,std::string,double>*>(ptr)->Get(*reinterpret_cast<vineyard::Client*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_getter_ArrowProjectedFragmentGetter_1cxx_10xdaece666_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowProjectedFragmentGetter<int64_t,uint64_t,std::string,double>());
}

#ifdef __cplusplus
}
#endif
