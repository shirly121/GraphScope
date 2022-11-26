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
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeEdgeLabel(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->edge_label());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeEdgePropId(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->edge_prop_id());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeFid(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->fid());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeFnum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->fnum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetArrowFragment(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<gs::ArrowFragmentDefault<int64_t>>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_arrow_fragment()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetEdataArrayAccessor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_edata_array_accessor()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetEdgeNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetFragId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetFragId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetIEOffsetsBeginPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_ie_offsets_begin_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetIEOffsetsEndPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_ie_offsets_end_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetInEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInEdgesPtr(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_in_edges_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetIncomingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetIncomingAdjList(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetIncomingEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetIncomingEdgeNum());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInnerVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetInnerVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInnerVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetInnerVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInnerVertexId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetInnerVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetInnerVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetInnerVerticesNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetLocalInDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetLocalInDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetLocalOutDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetLocalOutDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOEOffsetsBeginPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_oe_offsets_begin_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOEOffsetsEndPtr(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_oe_offsets_end_ptr());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOutEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOutEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOutEdgesPtr(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_out_edges_ptr());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOuterVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOuterVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOuterVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOuterVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOuterVertexId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOuterVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOuterVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOuterVerticesNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOutgoingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::arrow_projected_fragment_impl::AdjListDefault<uint64_t,int64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOutgoingAdjList(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetOutgoingEdgeNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetOutgoingEdgeNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetTotalVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetTotalVerticesNum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetVdataArrayAccessor(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->get_vdata_array_accessor()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetVertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGetVerticesNum(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->GetVerticesNum());
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->Gid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeId(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->id());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeIeDestList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::DestList(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->IEDests(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeInnerVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->InnerVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeInnerVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->InnerVertices()));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeIsInnerVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->IsInnerVertex(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeIsOuterVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->IsOuterVertex(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeOeDestList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::DestList(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->OEDests(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0))));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeOuterVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->OuterVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeOuterVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->OuterVertices()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeVertex2Gid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->Vertex2Gid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeVertexLabel(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->vertex_label());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeVertexPropId(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->vertex_prop_id());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowProjectedFragment_1cxx_10x50b609c9_nativeVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowProjectedFragment<int64_t,uint64_t,int32_t,int64_t>*>(ptr)->Vertices()));
}

#ifdef __cplusplus
}
#endif
