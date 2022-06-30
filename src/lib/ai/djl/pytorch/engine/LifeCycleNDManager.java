package ai.djl.pytorch.engine;

import ai.djl.Device;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.NDResource;

import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * A special Singleton NDManager that is direct child of the SystemNDManager.
 * It is not synchronized since access to it is by default single threaded from flink side
 * Has Tracker ability for nested NDArray creations, all other ndarrays are not kept in the reference queue
 * Internally one can create a child of this NDManager if DJL normal NDManager is needed
 * Try to avoid NDManager if it is not going to be closed again
 */
public class LifeCycleNDManager extends PtNDManager {
    private final transient HashMap<String, WeakReference<AutoCloseable>> registrations = new HashMap<>(10000); // Thread Local
    
    private final transient Scope scope = new Scope();

    private static final transient ThreadLocal<LifeCycleNDManager> instances = ThreadLocal.withInitial(()-> {
        System.out.println(Thread.currentThread().getName());
            return new LifeCycleNDManager(PtNDManager.getSystemManager(), PtNDManager.getSystemManager().getDevice());}); // Attached to the life cycle of the

    public LifeCycleNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    public static LifeCycleNDManager getInstance() {
        return instances.get();
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public void attachInternal(String resourceId, AutoCloseable resource) {
        registrations.putIfAbsent(resourceId, new WeakReference<>(resource));
    }

    @Override
    public void tempAttachInternal(NDManager originalManager, String resourceId, NDResource resource) {
        // Pass
    }


    @Override
    public void detachInternal(String resourceId) {
        registrations.remove(resourceId);
    }

    @Override
    public void close() {
        // Not closing explicitely
    }

    public void clean(){
        if(registrations.size() > 500) {
            registrations.forEach((key, val) -> {
                AutoCloseable tmp = val.get();
                if (tmp != null) {
                    try {
                        tmp.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            registrations.clear();
        }
    }

    /**
     * Context for doing ND operations so that input elements will be returned to their original managers after closing
     * Everything extra will be attached to this LifeCycleNDManager
     */
    public class Scope implements AutoCloseable{
        private transient NDList inputs;
        private final transient NDManager[] originalManagers = new NDManager[10];
        public Scope start(NDList inputs){
            this.inputs = inputs;
            for (int i = 0; i < this.inputs.size(); i++) {
                originalManagers[i] = inputs.get(i).getManager();
                inputs.get(i).attach(LifeCycleNDManager.this);
            }
            return this;
        }

        @Override
        public void close() throws Exception {
            for (int i = 0; i < inputs.size(); i++) {
                inputs.get(i).attach(originalManagers[i]);
            }
        }
    }

}