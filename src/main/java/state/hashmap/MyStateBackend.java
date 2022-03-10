package state.hashmap;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;

public class MyStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {
    public MyStateBackend() {
    }

    private MyStateBackend(MyStateBackend original, ReadableConfig config) {
        // configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
    }


    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @NotNull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws IOException {
        TaskStateManager taskStateManager = env.getTaskStateManager();
        LocalRecoveryConfig localRecoveryConfig = taskStateManager.createLocalRecoveryConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(metricGroup).build();
        return new HeapKeyedStateBackendBuilder<>(
                kvStateRegistry,
                keySerializer,
                env.getUserCodeClassLoader().asClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                env.getExecutionConfig(),
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                getCompressionDecorator(env.getExecutionConfig()),
                localRecoveryConfig,
                priorityQueueSetFactory,
                true,
                cancelStreamRegistry)
                .build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @NotNull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
        return new DefaultOperatorStateBackendBuilder(
                env.getUserCodeClassLoader().asClassLoader(),
                env.getExecutionConfig(),
                true,
                stateHandles,
                cancelStreamRegistry)
                .build();
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException {
        return new MyStateBackend(this, config);    }
}
