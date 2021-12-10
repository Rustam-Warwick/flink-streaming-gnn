package features;

import org.bytedeco.javacpp.annotation.Const;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.tensorflow.op.core.Constant;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import types.GraphElement;
import org.tensorflow.ndarray.DoubleNdArray;

public class ReplicableTFTensorFeature extends ReplicableFeature<TFloat32> {


    public ReplicableTFTensorFeature() {
        super();
    }

    public ReplicableTFTensorFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element, TFloat32 value) {
        super(fieldName, element, value);
    }

    @Override
    public void setValue(TFloat32 value) {
        this.editHandler(item->{
            item.value = value;
            return true;
        });
    }
}
