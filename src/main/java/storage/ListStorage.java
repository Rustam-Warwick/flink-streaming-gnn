package storage;

import elements.*;

import java.util.Map;
import java.util.stream.Stream;

public abstract class ListStorage extends BaseStorage{



    @Override
    public boolean addFeature(Feature feature) {
        return false;
    }

    @Override
    public boolean addVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean addEdge(Edge edge) {
        return false;
    }

    @Override
    public boolean updateFeature(Feature feature) {
        return false;
    }

    @Override
    public boolean updateVertex(Vertex vertex) {
        return false;
    }

    @Override
    public boolean updateEdge(Edge edge) {
        return false;
    }

    @Override
    public Vertex getVertex(String id) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;
    }

    @Override
    public Edge getEdge(String id) {
        return null;
    }

    @Override
    public Stream<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type) {
        return null;
    }

    @Override
    public Feature getFeature(String id) {
        return null;
    }

    @Override
    public Map<String, Feature> getFeatures(GraphElement e) {
        return null;
    }
}
