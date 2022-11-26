#include <jni.h>
#include <new>
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "core/java/type_alias.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowFragmentDefault<int64_t>);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeDirected(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->directed()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeEdgeLabelNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->edge_label_num());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeEdgePropertyNum(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->edge_property_num(arg0));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeFid(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->fid());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeFnum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->fnum());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetDoubleData(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetData<double>(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetFragId(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetFragId(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetIncomingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::AdjListDefault<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetIncomingAdjList(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetIncomingRawAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::RawAdjListDefault<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetIncomingRawAdjList(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0), arg1)));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetInnerVertex(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetInnerVertex(arg0, arg1, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetInnerVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetInnerVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetInnerVertexOid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetInnerVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetInnerVerticesNum(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetInnerVerticesNum(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetIntData(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetData<uint32_t>(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetLocalInDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetLocalInDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetLocalOutDegree(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetLocalOutDegree(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetLongData(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetData<uint64_t>(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOuterVertex(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOuterVertex(arg0, arg1, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOuterVertexGid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOuterVertexGid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOuterVertexOid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOuterVertexId(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOuterVerticesNum(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOuterVerticesNum(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOutgoingAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::AdjListDefault<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOutgoingAdjList(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetOutgoingRawAdjList(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) gs::RawAdjListDefault<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetOutgoingRawAdjList(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0), arg1)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetTotalNodesNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetTotalNodesNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetTotalVerticesNum0(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetTotalVerticesNum());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetTotalVerticesNum1(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetTotalVerticesNum(arg0));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetVertex(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetVertex(arg0, arg1, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGetVerticesNum(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->GetVerticesNum(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGid2Oid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->Gid2Oid(arg0));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->Gid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeHasChild(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->HasChild(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeHasParent(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->HasParent(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0), arg1)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeInnerVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->InnerVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeInnerVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->InnerVertices(arg0)));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeIsInnerVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->IsInnerVertex(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeIsOuterVertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->IsOuterVertex(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeOid2Gid(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */, jlong arg1 /* arg11 */, jlong arg2 /* arg22 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->Oid2Gid(arg0, arg1, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg2))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeOuterVertexGid2Vertex(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	return (reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->OuterVertexGid2Vertex(arg0, *reinterpret_cast<grape::Vertex<uint64_t>*>(arg1))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeOuterVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->OuterVertices(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertex2Gid(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->Vertex2Gid(*reinterpret_cast<grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertexLabel(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->vertex_label(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertexLabelNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->vertex_label_num());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertexOffset(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->vertex_offset(*reinterpret_cast<const grape::Vertex<uint64_t>*>(arg0)));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertexPropertyNum(JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
	return (jint)(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->vertex_property_num(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragment_1cxx_10x999c91c4_nativeVertices(JNIEnv*, jclass, jlong ptr, jlong rv_base, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) grape::VertexRange<uint64_t>(reinterpret_cast<gs::ArrowFragmentDefault<int64_t>*>(ptr)->Vertices(arg0)));
}

#ifdef __cplusplus
}
#endif
