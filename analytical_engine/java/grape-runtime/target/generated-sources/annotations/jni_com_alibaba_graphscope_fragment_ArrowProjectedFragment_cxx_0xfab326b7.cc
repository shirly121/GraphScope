#include <jni.h>
#include <new>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeEdgeLabel(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->edge_label());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeEdgePropId(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->edge_prop_id());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeFid(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->fid());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeFnum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->fnum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetArrowFragment(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowFragmentDefault<int64_t>>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_arrow_fragment()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetEdataArrayAccessor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_edata_array_accessor()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetEdgeNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetFragId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetFragId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetIEOffsetsBeginPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_ie_offsets_begin_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetIEOffsetsEndPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_ie_offsets_end_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetInEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInEdgesPtr(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_in_edges_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetIncomingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,std::string>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetIncomingAdjList(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetIncomingEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetIncomingEdgeNum());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInnerVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetInnerVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInnerVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetInnerVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInnerVertexId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetInnerVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetInnerVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetInnerVerticesNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetLocalInDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetLocalInDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetLocalOutDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetLocalOutDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOEOffsetsBeginPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_oe_offsets_begin_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOEOffsetsEndPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_oe_offsets_end_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOutEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOutEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOutEdgesPtr(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_out_edges_ptr());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOuterVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOuterVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOuterVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOuterVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOuterVertexId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOuterVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOuterVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOuterVerticesNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOutgoingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,std::string>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOutgoingAdjList(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetOutgoingEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetOutgoingEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetTotalVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetTotalVerticesNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetVdataArrayAccessor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->get_vdata_array_accessor()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGetVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->GetVerticesNum());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->Gid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeId(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->id());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeIeDestList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::DestList(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->IEDests(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeInnerVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->InnerVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeInnerVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->InnerVertices()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeIsInnerVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->IsInnerVertex(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeIsOuterVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->IsOuterVertex(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeOeDestList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::DestList(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->OEDests(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeOuterVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->OuterVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeOuterVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->OuterVertices()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeVertex2Gid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->Vertex2Gid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeVertexLabel(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->vertex_label());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeVertexPropId(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->vertex_prop_id());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10xfab326b7_nativeVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,std::string>*>(ptr)->Vertices()));
}

#ifdef __cplusplus
}
#endif
