package elements;

import iterations.IterationState;
import scala.Tuple2;

public class ReplicableGraphElement extends GraphElement {
    public short master = -1;
    public boolean halo = false;

    public ReplicableGraphElement(String id) {
        super(id);
    }

    public ReplicableGraphElement(String id, short part_id) {
        super(id, part_id);
    }

    @Override
    public Boolean createElement() {
        if (this.state() == ReplicaState.REPLICA){
            this.features.clear();
        }
        boolean is_created = super.createElement();
        if(is_created){
            if(this.state() == ReplicaState.MASTER){
                // Add setFeature
            }
            else{
                // Send Query
                this.storage.message(new GraphOp(Op.SYNC, this.masterPart(), this, IterationState.ITERATE));
            }
        }
        return is_created;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        if(this.state() == ReplicaState.MASTER){


        }else if(this.state() == ReplicaState.REPLICA){
            return this.updateElement(newElement);
        }

        return super.syncElement(this);

    }


    @Override
    public short masterPart() {
        return this.master;
    }

    @Override
    public Boolean isHalo() {
        return this.halo;
    }
}
