package operators.coordinators.events;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * Sent to master from last operator and called on latency marker IDLE
 * Notifies the operators to start the training
 */
public class StartTraining implements OperatorEvent {

}
