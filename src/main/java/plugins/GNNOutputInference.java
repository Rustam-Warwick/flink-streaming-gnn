package plugins;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.IterationType;
import scala.Tuple2;

import java.util.Objects;

public abstract class GNNOutputInference extends Plugin {
    public transient Model outputModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public int MODEL_VERSION = 0;
    public transient boolean updatePending = false;

    public GNNOutputInference() {
        super("inferencer");
    }

    public abstract Model createOutputModel();

    @Override
    public void add() {
        super.add();
        this.storage.withPlugin(new GNNOutputTraining());
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.loadModel(this.outputModel);
    }

    @Override
    public void open() {
        super.open();
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.restoreModel(this.outputModel);
        this.parameterStore.setNDManager(this.storage.manager.getLifeCycleManager());
    }

    @Override
    public void close() {
        super.close();
        this.outputModel.close();
    }

    public boolean outputReady(Edge edge) {
        return !updatePending && Objects.nonNull(edge.src.getFeature("feature")) && ((VTensor) edge.src.getFeature("feature")).isReady(MODEL_VERSION) && Objects.nonNull(edge.dest.getFeature("feature")) && ((VTensor) edge.dest.getFeature("feature")).isReady(MODEL_VERSION);
    }

    public NDArray output(NDArray feature, boolean training) {
        NDManager oldManager = feature.getManager();
        feature.attach(this.storage.manager.getTempManager());
        NDArray res = this.outputModel.getBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        feature.attach(oldManager);
        return res;
    }

    public void makePredictionAndSendForward(VTensor embedding) {
        NDArray res = this.output(embedding.getValue(), false);
        Vertex a = new Vertex(embedding.attachedTo._2);
        a.setFeature("logits", new VTensor(new Tuple2<>(res, 0)));
        this.storage.layerFunction.message(new GraphOp(Op.COMMIT, this.storage.layerFunction.getCurrentPart(), a, IterationType.FORWARD));
    }


}
