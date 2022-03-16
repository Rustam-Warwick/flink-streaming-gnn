package features;

import ai.djl.ndarray.NDArray;
import elements.Feature;
import elements.GraphElement;
import helpers.NDTensor;
import scala.Tuple2;

/**
 * Versioned Tensor, Used to represent embeddings of specific model versions
 */
public class VTensor extends Feature<Tuple2<NDTensor, Integer>, NDArray> {

    public VTensor() {
    }

    public VTensor(NDArray tmp) {
        super(new Tuple2<>(new NDTensor(tmp), 0));
    }
    public VTensor(NDArray tmp, int version) {
        super(new Tuple2<>(new NDTensor(tmp), version));
    }

    public VTensor(Tuple2<NDTensor, Integer> value) {
        super(value);
    }

    public VTensor(Tuple2<NDTensor, Integer> value, boolean halo) {
        super(value, halo);
    }

    public VTensor(Tuple2<NDTensor, Integer> value, boolean halo, short master) {
        super(value, halo, master);
    }

    public VTensor(String id, Tuple2<NDTensor, Integer> value) {
        super(id, value);
    }

    public VTensor(String id, Tuple2<NDTensor, Integer> value, boolean halo) {
        super(id, value, halo);
    }

    public VTensor(String id, Tuple2<NDTensor, Integer> value, boolean halo, short master) {
        super(id, value, halo, master);
    }


    @Override
    public GraphElement copy() {
        VTensor tmp = new VTensor(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        NDTensor copyArray = this.value._1.deepCopy();
        VTensor tmp = new VTensor(this.id, new Tuple2<>(copyArray, this.value._2), this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.element = this.element;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        return tmp;
    }

    @Override
    public NDArray getValue() {
        if(this.storage == null){
            System.out.println();
        }
        return this.value._1.get(this.storage.manager.getTempManager());
    }

    public boolean isReady(int modelVersion){
        return true;
    }
}
