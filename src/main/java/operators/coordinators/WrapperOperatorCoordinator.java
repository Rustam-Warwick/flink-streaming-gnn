package operators.coordinators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Operator coordinator for all the wrapper operators
 * In other words this operator coordinator actually manages a chain of operators in the GNN pipeline
 * Communication is done through the singleton subtaskGateways
 *
 * @todo can we have more than one such chains in one session? This only supports one. Good to also have a unique id for one stream of ML pipeline
 */
public class WrapperOperatorCoordinator implements OperatorCoordinator {

    protected static final HashMap<Short, SubtaskGateway[]> subtaskGateways = new HashMap<>(); // Subtask gateway for all operators in the chain
    protected final Context context;
    protected final short position;
    protected final short layers;
    private final HashMap<Class<? extends OperatorEvent>, WrapperOperatorEventHandler> handlers;


    public WrapperOperatorCoordinator(Context context, short position, short layers) {
        this.context = context;
        this.position = position;
        this.layers = layers;
        subtaskGateways.put(position, new SubtaskGateway[context.currentParallelism()]);
        handlers = new HashMap();
    }

    @Override
    public void start() throws Exception {
        handlers.values().forEach(item -> {
            try {
                item.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws Exception {
        handlers.values().forEach(item -> {
            try {
                item.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        WrapperOperatorEventHandler handler = handlers.getOrDefault(event.getClass(), null);
        if (Objects.nonNull(handler)) handler.handleEventFromOperator(subtask, event);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        // To be implemented
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // To be implemented
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        // To be implemented
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        handlers.values().forEach(item -> {
            try {
                item.subtaskFailed(subtask, reason);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        handlers.values().forEach(item -> {
            try {
                item.subtaskReset(subtask, checkpointId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void subtaskReady(int subtaskIndex, SubtaskGateway subtaskGateway) {
        subtaskGateways.get(position)[subtaskIndex] = subtaskGateway;
        handlers.values().forEach(item -> {
            try {
                item.subtaskReady(subtaskIndex, subtaskGateway);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Subscribe an event handler with this coordinator
     *
     * @param e Event Handler
     */
    public void subscribe(WrapperOperatorEventHandler e) {
        e.getEventClasses().forEach(aClass -> {
            handlers.put(aClass, e);
        });
        e.setCoordinator(this);
    }

    /**
     * The factory of {@link WrapperOperatorCoordinator}.
     */
    public static class HeadOperatorCoordinatorProvider implements Provider {

        private final OperatorID operatorId;
        private final short position;
        private final short layers;

        public HeadOperatorCoordinatorProvider(
                OperatorID operatorId, short position, short layers) {
            this.operatorId = operatorId;
            this.position = position;
            this.layers = layers;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            WrapperOperatorCoordinator tmp = new WrapperOperatorCoordinator(context, position, layers);
            tmp.subscribe(new TrainingEventHandler());
            return tmp;
        }
    }
}
