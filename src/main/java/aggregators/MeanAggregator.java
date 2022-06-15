package aggregators;

import ai.djl.ndarray.NDArray;
import elements.iterations.RemoteFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

public class MeanAggregator extends BaseAggregator<Tuple2<NDArray, Integer>> {

    public MeanAggregator() {
        super();
    }

    public MeanAggregator(MeanAggregator m, boolean deepCopy) {
        super(m, deepCopy);
    }

    public MeanAggregator(NDArray value, boolean halo) {
        this(new Tuple2<>(value, 0), halo, (short) -1);
    }

    public MeanAggregator(Tuple2<NDArray, Integer> value) {
        super(value);
    }

    public MeanAggregator(Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public MeanAggregator(String id, Tuple2<NDArray, Integer> value) {
        super(id, value);
    }

    public MeanAggregator(String id, Tuple2<NDArray, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    public static NDArray bulkReduce(NDArray... newElements) {
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        return sum;
    }

    @Override
    public MeanAggregator copy() {
        return new MeanAggregator(this, false);
    }

    @Override
    public MeanAggregator deepCopy() {
        return new MeanAggregator(this, true);
    }

    @RemoteFunction
    @Override
    public void reduce(NDArray newElement, int count) {
        NDArray newValue = this.value.f0.mul(this.value.f1).add(newElement).div(this.value.f1 + count);
        int newCount = this.value.f1 + count;
        this.value = new Tuple2<>(newValue, newCount);
    }

    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        NDArray newValue = value.f0.add((newElement.sub(oldElement)).div(value.f1));
        this.value = new Tuple2<>(newValue, value.f1);
    }

    @Override
    public NDArray grad(NDArray aggGradient, NDArray messages) {
        return aggGradient.div(value.f1).expandDims(0).repeat(0, messages.getShape().get(0)); // (batch_size, gradient)
    }

    @Override
    public NDArray getValue() {
        return value.f0;
    }

    @Override
    public void reset() {
        value = new Tuple2<>(value.f0.zerosLike(), 0);
    }
}
