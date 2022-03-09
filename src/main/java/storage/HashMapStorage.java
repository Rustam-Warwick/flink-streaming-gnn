package storage;

import elements.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;



public abstract class HashMapStorage extends BaseStorage{
    public transient MapState<String, Integer> translationTable;
    public transient MapState<Integer, String> reverseTranslationTable;
    public transient MapState<Integer, Vertex> vertexTable;
    public transient MapState<Integer, Feature> featureTable;
    public transient MapState<Integer, List<Integer>> elementFeatures;
    public transient MapState<Integer, List<Integer>> vertexOutEdges;
    public transient MapState<Integer, List<Integer>> vertexInEdges;
    public transient ValueState<Integer> lastId;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> translationTableDesc = new MapStateDescriptor<String, Integer>("translationTable",String.class, Integer.class);
        MapStateDescriptor<Integer, String> reverseTranslationTableDesc = new MapStateDescriptor("reverseTranslationTable",Integer.class, String.class);
        MapStateDescriptor<Integer, Vertex> vertexTableDesc = new MapStateDescriptor("vertexTable",Integer.class, Vertex.class);
        MapStateDescriptor<Integer, Feature> featureTableDesc = new MapStateDescriptor("featureTable",Integer.class, Feature.class);
        MapStateDescriptor<Integer, List<Integer>> elementFeaturesDesc = new MapStateDescriptor("elementFeatures",Integer.class, List.class);
        MapStateDescriptor<Integer, List<Integer>> vertexOutEdgesDesc = new MapStateDescriptor("vertexOutEdges",Integer.class, List.class);
        MapStateDescriptor<Integer, List<Integer>> vertexInEdgesDesc = new MapStateDescriptor("vertexInEdges",Integer.class, List.class);
        ValueStateDescriptor<Integer> lastIdDesc = new ValueStateDescriptor<Integer>("lastId", Integer.class);
        this.translationTable = getRuntimeContext().getMapState(translationTableDesc);
        this.reverseTranslationTable = getRuntimeContext().getMapState(reverseTranslationTableDesc);
        this.vertexTable = getRuntimeContext().getMapState(vertexTableDesc);
        this.featureTable = getRuntimeContext().getMapState(featureTableDesc);
        this.elementFeatures = getRuntimeContext().getMapState(elementFeaturesDesc);
        this.vertexOutEdges = getRuntimeContext().getMapState(vertexOutEdgesDesc);
        this.vertexInEdges = getRuntimeContext().getMapState(vertexInEdgesDesc);
        this.lastId = getRuntimeContext().getState(lastIdDesc);
        super.open(parameters);
    }

    private int getLastId() throws IOException {
        Integer last_id = this.lastId.value();
        if(last_id == null){
            last_id = 0;
        }
        this.lastId.update(last_id + 1); // Increment, better sooner than later
        return last_id;
    }

    @Override
    public boolean addFeature(Feature feature) {
        try{
            if(this.translationTable.contains(feature.getId()))return false;
            int last_id = this.getLastId();
            this.translationTable.put(feature.getId(),last_id);
            this.reverseTranslationTable.put(last_id, feature.getId());
            this.featureTable.put(last_id, feature);
            if(feature.attachedTo._1 != ElementType.NONE){
                // Feature belongs to some other element
                int elementId = this.translationTable.get((String) feature.attachedTo._2());
                if(!this.elementFeatures.contains(elementId)){
                    this.elementFeatures.put(elementId, new ArrayList<>());
                }
                List<Integer> featureIds = this.elementFeatures.get(elementId);
                featureIds.add(last_id);
                this.elementFeatures.put(elementId, featureIds);
            }
            return true;
        }catch (Exception e){
            return false;
        }

    }


    @Override
    public boolean addVertex(Vertex vertex){
        try{
            if(this.translationTable.contains(vertex.getId()))return false;
            int last_id = this.getLastId();
            this.translationTable.put(vertex.getId(),last_id);
            this.reverseTranslationTable.put(last_id, vertex.getId());
            this.vertexTable.put(last_id, vertex);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    @Override
    public boolean addEdge(Edge edge) {
       try{
           int srcIntId = this.translationTable.get(edge.src.getId());
           int destIntId = this.translationTable.get(edge.dest.getId());
           if(!this.vertexInEdges.contains(destIntId)){
               this.vertexInEdges.put(destIntId, new ArrayList<Integer>());
           }
           if(!this.vertexOutEdges.contains(srcIntId)){
               this.vertexOutEdges.put(srcIntId, new ArrayList<Integer>());
           }

           List<Integer> destInEdges = this.vertexInEdges.get(destIntId);
           destInEdges.add(srcIntId);
           this.vertexInEdges.put(destIntId, destInEdges);

           List<Integer> srcOutEdges = this.vertexOutEdges.get(srcIntId);
           srcOutEdges.add(destIntId);
           this.vertexOutEdges.put(srcIntId, srcOutEdges);
           return true;

       }catch (Exception e){
           return false;
       }
    }

    @Override
    public boolean updateFeature(Feature feature) {
        try{
            int featureId = this.translationTable.get(feature.getId());
            this.featureTable.put(featureId, feature);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        try{
            int vertexId = this.translationTable.get(vertex.getId());
            this.vertexTable.put(vertexId, vertex);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        try{
            int vertexId = this.translationTable.get(id);
            Vertex res = this.vertexTable.get(vertexId);
            res.setStorage(this);
            return res;
        }catch (Exception e){
            return null;
        }

    }

    @Override
    public Iterable<Vertex> getVertices() {
        try{
            return this.vertexTable.values();
        }catch (Exception e){
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
            int vertex_id = this.translationTable.get(vertex.getId());
            switch (edge_type){
                case IN:
                    return this.vertexInEdges.get(vertex_id).stream().map(srcId -> {
                        try {
                            Vertex src = this.vertexTable.get(srcId);
                            return new Edge(src, vertex);
                        } catch (Exception e) {
                            return null;
                        }
                    });
                case OUT:
                    return this.vertexOutEdges.get(vertex_id).stream().map(destId -> {
                        try {
                            Vertex dest = this.vertexTable.get(destId);
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
        try{
            int featureId = this.translationTable.get(id);
            Feature res = this.featureTable.get(featureId);
            res.setStorage(this);
            return res;
        }catch (Exception e){
            return null;
        }
    }

    @Override
    public Map<String, Feature> getFeatures(GraphElement e) {
        HashMap<String, Feature> result = new HashMap<>();
        try {

            int element_id = this.translationTable.get(e.getId());
            List<Integer> features_found = this.elementFeatures.get(element_id);
            for(int id: features_found){
                Feature tmp = this.featureTable.get(id);
                tmp.setStorage(this);
                result.put(tmp.id, tmp);
            }
        } catch (Exception ignored) {

        }
        return result;


    }


}
