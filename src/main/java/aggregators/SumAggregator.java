package aggregators;

import ai.djl.ndarray.NDArray;
import elements.GraphElement;
import helpers.JavaTensor;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashMap;

public class SumAggregator extends BaseAggregator<Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>>> {
    public SumAggregator() {
        super();
    }

    public SumAggregator(JavaTensor tensor, boolean halo) {
        this(new Tuple3<>(tensor, 0, new HashMap<>()), halo);
    }

    public SumAggregator(Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value) {
        super(value);
    }

    public SumAggregator(Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(value, halo);
    }

    public SumAggregator(Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public SumAggregator(String id, Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value) {
        super(id, value);
    }

    public SumAggregator(String id, Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value, boolean halo) {
        super(id, value, halo);
    }

    public SumAggregator(String id, Tuple3<JavaTensor, Integer, HashMap<Integer, Integer>> value, boolean halo, short master) {
        super(id, value, halo, master);
    }

    @Override
    public GraphElement copy() {
        SumAggregator tmp = new SumAggregator(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        SumAggregator tmp = (SumAggregator) this.copy();
        tmp.element = this.element;
        tmp.storage = this.storage;
        return tmp;
    }

    @RemoteFunction
    @Override
    public void reduce(NDArray newElement, int count) {
        this.value._1().addi(newElement);
        this.value = new Tuple3<>(this.value._1(), this.value._2() + count, this.value._3());
        if(this.attachedTo._2.equals("434")){
            System.out.println("Reduce count: "+count+"  NumOfAggElements: "+this.value._2()+"  In Storage Position: "+this.storage.position);
        }
    }

    @Override
    public void bulkReduce(NDArray... newElements) {
        if(newElements.length <= 0) return;
        NDArray sum = Arrays.stream(newElements).reduce(NDArray::addi).get();
        Rpc.call(this, "reduce", sum, newElements.length);
    }

    @RemoteFunction
    @Override
    public void replace(NDArray newElement, NDArray oldElement) {
        newElement.subi(oldElement);
        this.value._1().addi(newElement);
        this.value = new Tuple3<>(this.value._1(), this.value._2(), this.value._3());
    }

    @Override
    public NDArray grad() {
        return this.value._1().getGradient();
    }

    @Override
    public boolean isReady(int modelVersion) {
        return true;
    }

    @Override
    public void reset() {

    }

    @Override
    public NDArray getValue() {
        return this.value._1();
    }
}