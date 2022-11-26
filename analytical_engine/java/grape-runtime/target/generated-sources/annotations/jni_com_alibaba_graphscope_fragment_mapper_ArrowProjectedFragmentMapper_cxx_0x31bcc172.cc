#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment_mapper.h"
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_mapper_ArrowProjectedFragmentMapper_1cxx_10x31bcc172__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragmentMapper<int64_t,uint64_t,double,int64_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_mapper_ArrowProjectedFragmentMapper_1cxx_10x31bcc172_nativeMap0(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */, jint arg2 /* arg22 */, jlong arg3 /* arg33 */, jlong arg4 /* arg44 */, jlong arg5 /* arg55 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>(reinterpret_cast<gs::ArrowProjectedFragmentMapper<int64_t,uint64_t,double,int64_t>*>(ptr)->Map(*reinterpret_cast<std::shared_ptr<gs::ArrowFragmentDefault<int64_t>>*>(arg0), arg1, arg2, *reinterpret_cast<gs::ArrowArrayBuilder<double>*>(arg3), *reinterpret_cast<gs::ArrowArrayBuilder<int64_t>*>(arg4), *reinterpret_cast<vineyard::Client*>(arg5))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_mapper_ArrowProjectedFragmentMapper_1cxx_10x31bcc172_nativeMap1(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */, jint arg2 /* arg22 */, jlong arg3 /* arg33 */, jlong arg4 /* arg44 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>(reinterpret_cast<gs::ArrowProjectedFragmentMapper<int64_t,uint64_t,double,int64_t>*>(ptr)->Map(*reinterpret_cast<std::shared_ptr<gs::ArrowFragmentDefault<int64_t>>*>(arg0), arg1, arg2, *reinterpret_cast<gs::ArrowArrayBuilder<double>*>(arg3), *reinterpret_cast<vineyard::Client*>(arg4))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_mapper_ArrowProjectedFragmentMapper_1cxx_10x31bcc172_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowProjectedFragmentMapper<int64_t,uint64_t,double,int64_t>());
}

#ifdef __cplusplus
}
#endif
