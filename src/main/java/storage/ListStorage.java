package storage;

import elements.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ListStorage extends BaseStorage{
    HashMap<String, Feature> features = new HashMap<>();
    HashMap<String, Tuple2<Vertex, List<String>>> vertices = new HashMap<>();
    HashMap<String, List<String>> vertexOutEdges = new HashMap<>();
    HashMap<String, List<String>> vertexInEdges = new HashMap<>();


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
//        ValueStateDescriptor<HashMap> featureDescriptor = new ValueStateDescriptor<HashMap>("features", HashMap.class);
//        ValueStateDescriptor<HashMap> verticesDescriptor = new ValueStateDescriptor<HashMap>("vertices", HashMap.class);
//        ValueStateDescriptor<HashMap> vertexOutEdgesDescriptor = new ValueStateDescriptor<HashMap>("vertexOutEdges", HashMap.class);
//        ValueStateDescriptor<HashMap> vertexInEdgesDescriptor = new ValueStateDescriptor<HashMap>("vertexInEdges", HashMap.class);
//
//        this.features = context.getKeyedStateStore().getState()

    }

    @Override
    public boolean addFeature(Feature feature) {
        try {
            if(this.features.containsKey(feature.getId()))return false;
            this.features.put(feature.getId(), feature);
            if(feature.attachedTo._1 != ElementType.VERTEX){
                this.vertices.get(feature.attachedTo._2())._2.add(feature.getId());
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        try {
            if(this.vertices.containsKey(vertex.getId()))return false;
            this.vertices.put(vertex.getId(), new Tuple2<>(vertex, new ArrayList<>()));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
        try{
            if(!this.vertexInEdges.containsKey(edge.dest.getId())){
                this.vertexInEdges.put(edge.dest.getId(), new ArrayList<>());
            }
            if(!this.vertexOutEdges.containsKey(edge.src.getId())){
                this.vertexOutEdges.put(edge.src.getId(), new ArrayList<>());
            }
            List<String> destInEdges = this.vertexInEdges.get(edge.dest.getId());
            destInEdges.add(edge.src.getId());
            this.vertexInEdges.put(edge.dest.getId(), destInEdges);


            List<String> srcOutEdges = this.vertexOutEdges.get(edge.src.getId());
            srcOutEdges.add(edge.dest.getId());
            this.vertexOutEdges.put(edge.src.getId(), srcOutEdges);

            return true;
        }catch (Exception e){

            return false;
        }
    }

    @Override
    public boolean updateFeature(Feature feature) {
        try {
            if(!this.features.containsKey(feature.getId()))return false;
            this.features.put(feature.getId(), feature);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        try {
            if(!this.vertices.containsKey(vertex.getId()))return false;
            Tuple2<Vertex, List<String>> res = this.vertices.get(vertex.getId());
            this.vertices.put(vertex.getId(), new Tuple2<>(vertex, res._2));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        try {
            if(!this.vertices.containsKey(id))return null;
            Vertex res = this.vertices.get(id)._1();
            res.setStorage(this);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        try {
            return this.vertices.values().stream().map(item->item._1).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Edge getEdge(String id) {
        return null;
    }

    @Override
    public Stream<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        try{
            switch (edge_type){
                case IN:
                    return this.vertexInEdges.get(vertex.getId()).stream().map(srcId -> {
                        try {
                            Vertex src = this.getVertex(srcId);
                            return new Edge(src, vertex);
                        } catch (Exception e) {
                            return null;
                        }
                    });
                case OUT:
                    return this.vertexOutEdges.get(vertex.getId()).stream().map(destId -> {
                        try {
                            Vertex dest = this.getVertex(destId);
                            return new Edge(vertex, dest);
                        } catch (Exception e) {
                            return null;
                        }
                    });
                default:
                    return Stream.of();
            }
        }catch (Exception e){
            return Stream.of();
        }
    }

    @Override
    public Feature getFeature(String id) {
        try {
            if(!this.features.containsKey(id))return null;
             Feature res = this.features.get(id);
             res.setStorage(this);
             return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Map<String, Feature> getFeatures(GraphElement e) {
        HashMap<String, Feature> result = new HashMap<>();
        try {
            if (e.elementType() == ElementType.VERTEX) {
                List<String> features = this.vertices.get(e.getId())._2;
                for(String featureId:features){
                    Feature tmp = this.getFeature(featureId);
                    tmp.setStorage(this);
                    result.put(tmp.getFieldName(), tmp);
                }
            }

            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            return result;
        }
    }
}
