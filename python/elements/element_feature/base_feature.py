import abc
import copy
from abc import ABCMeta
from functools import cached_property
from decorators import rmi
from typing import TYPE_CHECKING, Tuple
import re
from exceptions import NestedFeaturesException
from elements import ReplicableGraphElement, ElementTypes, GraphElement, ReplicaState, GraphQuery, query_for_part, Op


class ReplicableFeature(ReplicableGraphElement, metaclass=ABCMeta):
    """ Base class for all the features of Edge,Vertex or more upcoming updates
        Most of its features are combing from the associated GraphElement, whereas an ElementFeature is also a GraphElement
        @todo make this generic maybe ? To infer weather it is replicable or not: Hard to do without overheads
    """

    def __init__(self, element: "GraphElement" = None, element_id: str = None, value: object = None, *args, **kwargs):
        self._value = value
        self.element: "GraphElement" = element
        element_id = "%s%s" % (ElementTypes.FEATURE.value, element_id)
        super(ReplicableFeature, self).__init__(element_id=element_id, *args, **kwargs)

    def create_element(self) -> bool:
        if self.attached_to[0] is ElementTypes.NONE:
            # Independent feature behave just like ReplicableGraphElements
            is_created = super(ReplicableFeature, self).create_element()
        else:
            if self.element is None:
                # Make sure that element is here
                self.element = self.storage.get_element_by_id(self.attached_to[1])
            is_created = GraphElement.create_element(self)
            if is_created:
                if self.state is ReplicaState.MASTER: self.sync_replicas(skip_halos=False)
        return is_created

    def __call__(self, rmi: "Rpc") -> Tuple[bool, "GraphElement"]:
        """ Similar to sync_element we need to save the .element since integer_clock might change """
        is_updated, memento = super(ReplicableFeature, self).__call__(rmi)
        if is_updated and self.element is not None:
            self.storage.update_element(self.element)
        return is_updated, memento

    @rmi()
    def update_value(self, value, part_id, part_version):
        """ Rpc call to update the value field """
        self._value = value

    def update_element(self, new_element: "ReplicableFeature") -> Tuple[bool, "GraphElement"]:
        """ Similar to Graph Element  but added value swapping and no sub-feature checks """
        memento = copy.copy(self)  # .element field will be empty
        if new_element._value is None and self._value is None:
            is_updated = False
        elif new_element._value is None or self._value is None:
            is_updated = True
        else:
            is_updated = not self._value_eq_(self._value, new_element._value)
        if is_updated:
            self._value = new_element._value
            self.storage.update_element(self)
            self.storage.for_aggregator(lambda x: x.update_element_callback(self, memento))
        return is_updated, memento

    @abc.abstractmethod
    def _value_eq_(self, old_value, new_value) -> bool:
        """ Given @param(values) of 2 Features if they are equal  """
        pass

    @cached_property
    def field_name(self):
        """ Retrieve field name from the id """
        group = re.search("(\w+:)*(?P<feature_name>\w+)$", self.id[1:])
        if group:
            return group['feature_name']
        raise KeyError

    @cached_property
    def attached_to(self) -> Tuple["ElementTypes", str]:
        group = re.search("(?P<element_type>\w+):(?P<element_id>\w+):\w+", self.id[1:])
        if group:
            return ElementTypes(int(group['element_type'])), group['element_id']
        return (ElementTypes.NONE, None)  # Represents Standalone Feature

    @property
    def element_type(self):
        return ElementTypes.FEATURE

    @property
    def value(self):
        return self._value

    @property
    def master_part(self) -> int:
        if self.element:
            return self.element.master_part
        return super(ReplicableFeature, self).master_part

    @property
    def replica_parts(self) -> list:
        if self.element:
            return self.element.replica_parts
        return super(ReplicableFeature, self).replica_parts

    def sync_replicas(self, part_id=None, skip_halos=True):
        """ Make sure is_halo Features send None and _value  """
        if self.state is not ReplicaState.MASTER or (skip_halos and self.is_halo) or (len(
                self.replica_parts) == 0 and not part_id): return
        if self.attached_to[0] is ElementTypes.NONE:
            super(ReplicableFeature, self).sync_replicas(part_id, skip_halos)
        else:
            cpy_self = copy.copy(self)
            cpy_self._features.clear()
            if cpy_self.is_halo:
                cpy_self._value = None

            query = GraphQuery(op=Op.SYNC, element=cpy_self, part=None, iterate=True)
            if part_id:
                self.storage.message(query_for_part(query, part_id))
            else:
                filtered_parts = map(lambda x: query_for_part(query, x), self.replica_parts)
                for msg in filtered_parts:
                    self.storage.message(msg)

    def __setitem__(self, key, value):
        if self.attached_to[0] is ElementTypes.NONE:
            super(ReplicableFeature, self).__setitem__(key, value)
        else:
            pass

    def __getitem__(self, item):
        if self.attached_to[0] is ElementTypes.NONE:
            return super(ReplicableFeature, self).__getitem__(item)
        else:
            raise NestedFeaturesException

    def __deepcopy__(self, memodict={}):
        element = super(ReplicableFeature, self).__copy__()
        element.__dict__.update({
            "_value": copy.deepcopy(self._value),
            "element": copy.copy(self.element)
        })
        return element

    def __copy__(self):
        element = super(ReplicableFeature, self).__copy__()
        element.__dict__.update({
            "_value": self._value,
            "element": copy.copy(self.element)
        })
        return element

    def __getstate__(self):
        """ Fill in from the state """
        state = super(ReplicableFeature, self).__getstate__()
        state.update({
            "_value": self.value,
            "element": None  # No need to serialize element value
        })
        return state

    def __get_save_data__(self):
        meta_data = super(ReplicableFeature, self).__get_save_data__()
        meta_data.update({
            "_value": self._value
        })
        return meta_data
