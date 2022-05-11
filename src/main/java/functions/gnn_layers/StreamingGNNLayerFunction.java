package functions.gnn_layers;

import elements.GraphOp;
import iterations.MessageCommunication;
import iterations.MessageDirection;
import operators.BaseWrapperOperator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import storage.BaseStorage;

import java.io.IOException;

public class StreamingGNNLayerFunction extends KeyedProcessFunction<String, GraphOp, GraphOp> implements GNNLayerFunction {
    public BaseStorage storage;
    public transient short currentPart;
    public transient Collector<GraphOp> collector;
    public transient KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx;
    public transient BaseWrapperOperator<?>.Context baseWrapperContext;

    public StreamingGNNLayerFunction(BaseStorage storage) {
        this.storage = storage;
        storage.layerFunction = this;
    }

    @Override
    public short getCurrentPart() {
        return currentPart;
    }

    @Override
    public void setCurrentPart(short part) {
        this.currentPart = part;
    }

    @Override
    public BaseWrapperOperator<?>.Context getWrapperContext() {
        return baseWrapperContext;
    }

    @Override
    public void setWrapperContext(BaseWrapperOperator<?>.Context context) {
        this.baseWrapperContext = context;
    }

    @Override
    public void message(GraphOp op, MessageDirection direction) {
        try {
            if (direction == MessageDirection.BACKWARD) {
                ctx.output(BaseWrapperOperator.backwardOutputTag, op);
            } else if (direction == MessageDirection.FORWARD) {
                collector.collect(op);
            } else {
                ctx.output(BaseWrapperOperator.iterateOutputTag, op);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void broadcastMessage(GraphOp op, MessageDirection direction) {
        op.setMessageCommunication(MessageCommunication.BROADCAST);
        try {
            if (direction == MessageDirection.BACKWARD) {
                getWrapperContext().broadcastElement(BaseWrapperOperator.backwardOutputTag, new StreamRecord<>(op, op.getTimestamp()));
            } else if (direction == MessageDirection.FORWARD) {
                getWrapperContext().broadcastElement(null, new StreamRecord<>(op, op.getTimestamp()));
            } else {
                getWrapperContext().broadcastElement(BaseWrapperOperator.iterateOutputTag, new StreamRecord<>(op, op.getTimestamp()));
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <OUT> void sideBroadcastMessage(OUT op, OutputTag<OUT> outputTag) {
        getWrapperContext().broadcastElement(outputTag, new StreamRecord<>(op, currentTimestamp()));
    }

    @Override
    public <OUT> void sideMessage(OUT op, OutputTag<OUT> outputTag) {
        ctx.output(outputTag, op);
    }

    @Override
    public BaseStorage getStorage() {
        return storage;
    }

    @Override
    public void setStorage(BaseStorage storage) {
        this.storage = storage;
    }

    @Override
    public long currentTimestamp() {
        return ctx.timestamp() == null ? 0 : ctx.timestamp();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getStorage().open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        getStorage().close();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, GraphOp, GraphOp>.OnTimerContext ctx, Collector<GraphOp> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        getStorage().onTimer(timestamp);
    }

    @Override
    public TimerService getTimerService() {
        return ctx.timerService();
    }

    @Override
    public void processElement(GraphOp value, KeyedProcessFunction<String, GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
        setCurrentPart(Short.parseShort(ctx.getCurrentKey()));
        if (this.collector == null) this.collector = out;
        if (this.ctx == null) this.ctx = ctx;
        process(value);
    }
}
