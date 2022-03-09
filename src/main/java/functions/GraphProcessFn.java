package functions;

import elements.ElementType;
import elements.GraphElement;
import elements.GraphOp;
import elements.Op;
import iterations.IterationState;
import iterations.Rpc;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import storage.HashMapStorage;
import storage.ListStorage;

import java.util.Objects;

public class GraphProcessFn extends ListStorage {
    public transient Collector<GraphOp> out;

    @Override
    public void message(GraphOp op) {
        this.out.collect(op);
    }

    @Override
    public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        this.out = out;
        try{
            switch (value.op){
                case COMMIT:
                    GraphElement thisElement = this.getElement(value.element);
                    if(Objects.isNull(thisElement)){
                        if(!this.isLast() && (value.element.elementType()== ElementType.EDGE || value.element.elementType()== ElementType.VERTEX)){
                            this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                        }
                        value.element.setStorage(this);
                        value.element.createElement();
                    }
                    else{
                        thisElement.externalUpdate(value.element);
                    }
                    break;
                case SYNC:
                    GraphElement el = this.getElement(value.element);
                    if(Objects.isNull(el)){
                        if(!this.isLast() && (value.element.elementType()== ElementType.EDGE || value.element.elementType()== ElementType.VERTEX)){
                            this.message(new GraphOp(Op.COMMIT, this.currentKey, value.element.copy(), IterationState.FORWARD));
                        }
                        el = value.element.copy();
                        el.setStorage(this);
                        el.createElement();
                    }
                    el.syncElement(value.element);
                    break;
                case RPC:
                    GraphElement rpcElement = this.getElement(value.element.getId(),value.element.elementType());
                    Rpc.execute(rpcElement, (Rpc) value.element);
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
