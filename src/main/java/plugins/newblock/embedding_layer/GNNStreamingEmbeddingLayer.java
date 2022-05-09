package plugins.newblock.embedding_layer;

import aggregators.NewMeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import elements.*;
import features.Tensor;
import functions.nn.JavaTensor;
import functions.nn.MyParameterStore;
import functions.nn.SerializableModel;
import functions.nn.gnn.GNNBlock;
import iterations.MessageDirection;
import iterations.RemoteInvoke;

import java.util.Objects;

public class GNNStreamingEmbeddingLayer extends Plugin {
    // ---------------------- MODEL ---------------------
    public SerializableModel<GNNBlock> model;
    public transient Shape inputShape;
    public transient MyParameterStore parameterStore;
    // ---------------------- RUNTIME -------------------
    public boolean externalFeatures; // Do we expect external features or have random feature matrices
    public boolean ACTIVE = true; // Is the plugin currently running

    public GNNStreamingEmbeddingLayer(SerializableModel<GNNBlock> model, boolean externalFeatures) {
        super("inferencer");
        this.externalFeatures = externalFeatures;
        this.model = model;
    }


    @Override
    public void open() {
        super.open();
        model.setManager(storage.manager.getLifeCycleManager());
        parameterStore = new MyParameterStore(storage.manager.getLifeCycleManager());
        parameterStore.loadModel(model);
        inputShape = model.describeInput().get(0).getValue();
    }

    @Override
    public void close() {
        super.close();
        model.close();
    }

    @Override
    @SuppressWarnings("all")
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            if (ACTIVE && messageReady(edge)) {
                NDArray msg = this.message((NDArray) edge.src.getFeature("feature").getValue(), false);
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msg, 1)
                        .withTimestamp(edge.getTimestamp())
                        .buildAndRun(storage);
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature feature = (Feature) element;
            if ("feature".equals(feature.getName()) && ACTIVE) {
                ((Tensor) element).getValue().attach(storage.manager.getLifeCycleManager()); // Attach to life cycle manager
                reduceOutEdges((Vertex) feature.getElement());
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if ("feature".equals(feature.getName()) && ACTIVE) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                forward((Vertex) feature.getElement());
            }
            if ("agg".equals(feature.getName()) && ACTIVE) {
                forward((Vertex) feature.getElement());
            }
        }
    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     *
     * @param element Vertex to be initialized
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = this.storage.manager.getLifeCycleManager().zeros(inputShape);
            element.setFeature("agg", new NewMeanAggregator(new JavaTensor(aggStart), true), storage.layerFunction.currentTimestamp());

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = this.storage.manager.getLifeCycleManager().randomNormal(inputShape); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("feature", new Tensor(new JavaTensor(embeddingRandom)), storage.layerFunction.currentTimestamp());
            }
        }
    }

    /**
     * Push the embedding of this vertex to the next layer
     * After first layer, this is only fushed if agg and features are in sync
     *
     * @param v Vertex
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        if (updateReady(v) && (storage.layerFunction.isFirst() || v.getFeature("feature").getTimestamp() == v.getFeature("agg").getTimestamp())) {
            NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
            NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
            NDArray update = this.update(ft, agg, false);
            Vertex messageVertex = v.copy();
            long timestamp = v.getFeature("agg").getTimestamp();
            messageVertex.setFeature("feature", new Tensor(update), timestamp);
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex, MessageDirection.FORWARD, timestamp));
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                if (Objects.isNull(msgOld)) {
                    msgOld = this.message(oldFeature.getValue(), false);
                    msgNew = this.message(newFeature.getValue(), false);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("replace")
                        .hasUpdate()
                        .withTimestamp(edge.getTimestamp())
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msgNew, msgOld)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Given vertex reduce all the out edges aggregator values
     *
     * @param vertex Vertex which out edges should be reduces
     */
    public void reduceOutEdges(Vertex vertex) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        NDArray msg = null;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                if (Objects.isNull(msg)) {
                    msg = this.message((NDArray) vertex.getFeature("feature").getValue(), false);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.dest.masterPart())
                        .withTimestamp(edge.getTimestamp())
                        .withArgs(msg, 1)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature
     * @param agg      Aggregator Feature
     * @param training training enabled
     * @return Next layer feature
     */
    public NDArray update(NDArray feature, NDArray agg, boolean training) {
        NDManager oldFeatureManager = feature.getManager();
        NDManager oldAggManager = agg.getManager();
        NDManager tmpManager = storage.manager.getTempManager().newSubManager();
        feature.attach(tmpManager);
        agg.attach(tmpManager);
        NDArray res = model.getBlock().getUpdateBlock().forward(this.parameterStore, new NDList(feature, agg), training).get(0);
        res.attach(storage.manager.getTempManager());
        feature.attach(oldFeatureManager);
        agg.attach(oldAggManager);
        tmpManager.close();
        return res;
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param feature  Source Vertex feature
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public NDArray message(NDArray feature, boolean training) {
        NDManager oldManager = feature.getManager();
        NDManager tmpManager = storage.manager.getTempManager().newSubManager();
        feature.attach(tmpManager);
        NDArray res = model.getBlock().getMessageBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        res.attach(storage.manager.getTempManager());
        feature.attach(oldManager);
        tmpManager.close();
        return res;
    }

    /**
     * @param edge Edge
     * @return Is the Edge ready to pass on the message
     */
    public boolean messageReady(Edge edge) {
        return Objects.nonNull(edge.src.getFeature("feature"));
    }

    /**
     * @param vertex Vertex
     * @return Is the Vertex ready to be updated
     */
    public boolean updateReady(Vertex vertex) {
        return vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && Objects.nonNull(vertex.getFeature("agg"));
    }
}
