package plugins;

import aggregators.BaseAggregator;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.util.Pair;
import elements.*;
import features.VTensor;
import helpers.JavaTensor;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.util.HashMap;

public class GNNLayerTraining extends Plugin {
    public GNNLayerInference inference = null;
    public GNNLayerTraining(){
        super("trainer");
    }



    @RemoteFunction
    public void backward(VTensor grad){
        // 1. Get Data
        grad.setStorage(this.storage);
        VTensor feature = (VTensor) grad.getElement().getFeature("feature");
        BaseAggregator<?> agg = (BaseAggregator<?>) grad.getElement().getFeature("agg");
        feature.getValue().setRequiresGradient(true);
        agg.getValue().setRequiresGradient(true);

        // 2. Prediction & Backward
        NDArray prediction = this.inference.updateModel.getBlock().forward(this.inference.parameterStore, new NDList(feature.getValue(), agg.getValue()), true).get(0);
        JniUtils.backward((PtNDArray) prediction, null, false, false);

        // 3. Send Update backward if this is not last
        if(!this.storage.isFirst()){
            grad.value = new Tuple2<>(JavaTensor.of(feature.getValue().getGradient()), 0);
            Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
            this.storage.message(new GraphOp(Op.RPC, this.storage.currentKey, backward, IterationState.BACKWARD));
        }

        // 4. Send to messageBackward to do the message backward steps
//        Tensor aggGrad = (Tensor) grad.copy();
//        aggGrad.value = agg.grad();
//        Rpc.callProcedure(this, "messageBackward", IterationState.ITERATE, agg.replicaParts(), aggGrad);
//        this.messageBackward(aggGrad);

        // 5. Cleanup
        agg.getValue().setRequiresGradient(false);
        feature.getValue().setRequiresGradient(false);
    }

    @RemoteFunction
    public void messageBackward(JavaTensor aggGrad){
//        Vertex vertex = (Vertex) aggGrad.getElement();
//        Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
//        for(Edge edge: inEdges){
//            if(this.inference.messageReady(edge)){
//                ((NDArray) edge.src.getFeature("feature").getValue()).setRequiresGradient(true);
//                NDArray prediction = this.inference.messageModel.getBlock().forward(this.inference.parameterStore, new NDList((NDArray) edge.src.getFeature("feature").getValue()), true).get(0);
//                JniUtils.backward((PtNDArray) prediction, (PtNDArray) aggGrad.getValue(), false, false);
//                if(!this.storage.isFirst()){
//                    Tensor grad = new Tensor("grad", ((NDArray) edge.src.getFeature("feature").getValue()).getGradient());
//                    grad.attachedTo = new Tuple2<>(edge.src.elementType(), edge.src.getId());
//                    Rpc backward = new Rpc("trainer", "backward", new Object[]{grad}, ElementType.PLUGIN, false);
//                    this.storage.message(new GraphOp(Op.RPC, edge.src.masterPart(), backward, IterationState.BACKWARD));
//                }
//                ((NDArray) edge.src.getFeature("feature").getValue()).setRequiresGradient(false);
//            }
//        }
    }

    @RemoteFunction
    public void updateParameters(HashMap<String, Parameter> incoming){

    }

    @Override
    public void open() {
        super.open();
        inference = (GNNLayerInference) this.storage.getPlugin("inferencer");

    }
}
