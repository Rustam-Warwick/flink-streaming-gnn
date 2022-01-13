import operator

from elements import ReplicaState
from elements.graph_element import GraphElement, GraphQuery, Op
from elements.element_feature.set_feature import PartSetElementFeature
from typing import TYPE_CHECKING, Tuple
from exceptions import OldVersionException, NotUsedOnReplicaException

if TYPE_CHECKING:
    from storage.process_fn import GraphStorageProcess


class ReplicableGraphElement(GraphElement):
    """ General Graph element that is replicatable. Used in Vertex for now can be added for other things as well """

    def __init__(self, master: int = None, is_halo=False, *args, **kwargs):
        super(ReplicableGraphElement, self).__init__(*args, **kwargs)
        self.master = master
        self._clock = 0
        self._halo = is_halo

    def __call__(self, rpc: "Rpc") -> bool:
        if self.state == ReplicaState.REPLICA:
            # Send this message to master node if it is replica
            query = GraphQuery(op=Op.RPC, element=rpc, part=self.master_part, iterate=True)
            self.storage.message(query)
            return False
        is_updated = super(ReplicableGraphElement, self).__call__(rpc)
        if is_updated:
            self.sync_replicas()
        return is_updated

    def create_element(self) -> bool:
        is_created = super(ReplicableGraphElement, self).create_element()  # Store
        if not is_created: return is_created
        if self.state is ReplicaState.MASTER:
            self['parts'] = PartSetElementFeature({self.storage.part_id})
        elif self.state is ReplicaState.REPLICA:
            query = GraphQuery(Op.SYNC, self, self.master_part, True)
            self.storage.message(query)
        return is_created

    def sync_element(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.MASTER:
            """ Add to parts and sync with replicas """
            self["parts"].add(new_element.part_id)  # This is going to handle syncing no need to bother
            return False, self
        elif self.state is ReplicaState.REPLICA:
            if new_element.integer_clock < self.integer_clock: raise OldVersionException
            return self.update_element(new_element)

    def external_update(self, new_element: "GraphElement") -> Tuple[bool, "GraphElement"]:
        if self.state is ReplicaState.REPLICA:
            query = GraphQuery(Op.UPDATE, new_element, self.master_part, True)
            self.storage.message(query)
            return False, self
        is_updated, memento = super(ReplicableGraphElement, self).external_update(new_element)  #Basically calling update_element
        if is_updated and self.state is ReplicaState.MASTER:
            self.sync_replicas()
        return is_updated, memento

    def __iter__(self):
        tmp = super(ReplicableGraphElement, self).__iter__()
        return filter(lambda x: x[0] != "parts", tmp)

    @property
    def master_part(self) -> int:
        return self.master

    def get_integer_clock(self):
        return self._clock

    def set_integer_clock(self, value: int):
        self._clock = value

    def del_integer_clock(self):
        del self._clock

    @property
    def is_halo(self) -> bool:
        return self._halo

    @property
    def replica_parts(self) -> list:
        re: "PartSetElementFeature" = self['parts']
        a = list(re.value)
        a.remove(self.part_id)
        return a

    @property
    def is_replicable(self) -> bool:
        return True

    def __getstate__(self):
        """ No need to serialize the parts """
        state = super(ReplicableGraphElement, self).__getstate__()
        if "parts" in state["_features"]:
            del state["_features"]["parts"]
        return state
