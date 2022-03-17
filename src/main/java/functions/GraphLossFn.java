package functions;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.Loss;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import elements.ElementType;
import elements.GraphOp;
import elements.Op;
import features.Tensor;
import helpers.TaskNDManager;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;


/**
 * Function that takes in embeddigs and features and produces loss and
 */
public class GraphLossFn extends RichJoinFunction<GraphOp, GraphOp, GraphOp> {
    public Loss loss = null;
    public TaskNDManager manager;
    public Loss lossFunction(){
        return new SoftmaxCrossEntropyLoss();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.loss = lossFunction();
        this.manager = new TaskNDManager();
    }

    @Override
    public GraphOp join(GraphOp first, GraphOp second) throws Exception {
        // 1. Prepare data
        NDManager manager = NDManager.newBaseManager();
        GradientCollector collector = manager.getEngine().newGradientCollector();
        NDArray logit = ((NDArray) first.element.getFeature("logits").getValue()).expandDims(0);
        logit.setRequiresGradient(true);
        Integer tmp = (Integer) second.element.getFeature("label").getValue();
        NDArray label = manager.create(tmp).expandDims(0);

        // 2. Loss and backward
        NDArray loss = this.loss.evaluate(new NDList(label), new NDList(logit));
        collector.backward(loss);

        // 3. Prepare send data
        Tensor grad = new Tensor("grad", logit.getGradient());
        grad.attachedTo = new Tuple2<>(first.element.elementType(), first.element.getId());
        Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);

        // 4. Cleanup
        collector.close();
        manager.close();
        logit.setRequiresGradient(false);

        return new GraphOp(Op.RPC, first.part_id, backward, IterationState.BACKWARD);
    }
}
