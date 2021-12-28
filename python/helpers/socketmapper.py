from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from elements.vertex import SimpleVertex
from elements.edge import SimpleEdge
from elements import GraphQuery, Op


class EdgeListParser(MapFunction):
    def map(self, value: str) -> GraphQuery:
        values = value.split("\t")
        a = SimpleVertex(element_id=values[0])
        b = SimpleVertex(element_id=values[1])
        edge = SimpleEdge(src=a, dest=b)
        query = GraphQuery(Op.ADD, edge)
        return query
