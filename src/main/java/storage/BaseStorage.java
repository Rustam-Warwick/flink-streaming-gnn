package storage;

import ai.djl.ndarray.NDManager;
import elements.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

abstract public class BaseStorage extends ProcessFunction<GraphOp, GraphOp> implements CheckpointedFunction {
    public transient short currentKey = -1;
    public short parallelism = 1;
    public short position = 1;
    public short layers = 1;
    public final HashMap<String, Plugin> plugins = new HashMap<>();
    public static NDManager tensorManager;
    public abstract boolean addFeature(Feature feature);
    public abstract boolean addVertex(Vertex vertex);
    public abstract boolean addEdge(Edge edge);
    public abstract boolean updateFeature(Feature feature);
    public abstract boolean updateVertex(Vertex vertex);
    public abstract boolean updateEdge(Edge edge);
    public abstract Vertex getVertex(String id);
    public abstract Iterable<Vertex> getVertices();
    public abstract Edge getEdge(String id);
    public abstract Stream<Edge> getIncidentEdges(Vertex vertex, EdgeType edge_type);
    public abstract Feature getFeature(String id);
    public abstract Map<String, Feature> getFeatures(GraphElement e);

    public abstract void message(GraphOp op);
    public boolean isLast(){
        return this.position >= this.layers;
    }
    public boolean isFirst(){
        return this.position == 1;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.parallelism = (short) getRuntimeContext().getNumberOfParallelSubtasks();
        this.currentKey = (short) getRuntimeContext().getIndexOfThisSubtask();
        BaseStorage.tensorManager = NDManager.newBaseManager();
        this.plugins.values().forEach(item->{item.setStorage(this);item.open();});
    }

    @Override
    public void close() throws Exception {
        super.close();
        BaseStorage.tensorManager.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    public boolean addElement(GraphElement element){
        switch (element.elementType()){
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((Edge) element);
            case FEATURE:
                return this.addFeature((Feature<?, ?>) element);
            default:
                return false;
        }
    }

    public boolean updateElement(GraphElement element){
        switch (element.elementType()){
            case VERTEX:
                return this.updateVertex((Vertex) element);
            case EDGE:
                return this.updateEdge((Edge) element);
            case FEATURE:
                return this.updateFeature((Feature) element);
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element){
        return this.getElement(element.getId(), element.elementType());
    }

    public GraphElement getElement(String id, ElementType t){
        switch (t){
            case VERTEX:
                return this.getVertex(id);
            case FEATURE:
                return this.getFeature(id);
            case EDGE:
                return this.getEdge(id);
            case PLUGIN:
                return this.getPlugin(id);
            default:
                return null;
        }
    }

    /**
     * Adds Plugins to storage engine, this will be recorded in the state after open()
     * @param plugin Plugin to be added to the storage engine
     * @return
     */
    public BaseStorage withPlugin(Plugin plugin) {
        this.plugins.put(plugin.getId(), plugin);
        return this;
    }

    public Plugin getPlugin(String id){
        return this.plugins.get(id);
    }

    public Stream<Plugin> getPlugins(){
        return this.plugins.values().stream();
    }

}
