import os
import unittest

from py2neo import Graph, Node, Relationship, order, size

from adcp import Adcp


def load_test_nodes(neo4j: Graph):
    nodes = dict()

    tx = neo4j.begin()
    with open('tests/nodes.tsv', 'r') as ifile:
        for line in ifile.readlines():
            item, item_type = line.strip().split('\t')
            node = Node(item_type, name=item, is_test_node=True)
            nodes[item] = node
            tx.create(node)
    tx.commit()
    return nodes


def delete_test_nodes(neo4j: Graph):
    neo4j.run('MATCH (n) WHERE n.is_test_node DETACH DELETE n')
    return


def load_test_relations(neo4j: Graph, nodes: dict, filename: str):
    relations = []

    tx = neo4j.begin()
    with open('tests/{}.tsv'.format(filename), 'r') as ifile:
        for line in ifile.readlines():
            source, dest, rel = line.strip().split('\t')
            relation = Relationship(nodes[source], rel, nodes[dest], is_test_relation=True)
            relations.append(relation)
            tx.create(relation)
    tx.commit()
    return relations


class TestAdcp(object):
    class BaseTest(unittest.TestCase):
        _adcp = None  # type: Adcp

        def test_id(self):
            node = self._adcp.id('TEST_1')
            self.assertIsNotNone(node)

            node = self._adcp.id('TEST_15')
            self.assertIsNotNone(node)

            node = self._adcp.id('TEST_100')
            self.assertIsNone(node)

        def test_search(self):
            results = [x for x in self._adcp.search('TEST_1', operator='=')]
            self.assertEqual(len(results), 1)

            results = [x for x in self._adcp.search('TEST_1')]
            # Nodes 1 and 10 to 19
            self.assertEqual(len(results), 11)


class TestAdcp1(TestAdcp.BaseTest):
    @classmethod
    def setUpClass(cls):
        neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
        cls._adcp = Adcp(neo4j_uri=neo4j_uri)
        nodes = load_test_nodes(cls._adcp.neo4j)
        relations = load_test_relations(cls._adcp.neo4j, nodes, 'rels_1')

    @classmethod
    def tearDownClass(cls):
        delete_test_nodes(cls._adcp.neo4j)

    def test_empty_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_11', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        self.assertIsNone(graph)

    def test_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_1', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        # 1 GROUP : Nodes TEST_1 and TEST_10 to TEST_19
        self.assertEqual(order(graph), 11)

        # 1 GROUP Links from (TEST_10 to TEST_19) to TEST_1
        self.assertEqual(size(graph), 10)


class TestAdcp2(TestAdcp.BaseTest):
    @classmethod
    def setUpClass(cls):
        neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
        cls._adcp = Adcp(neo4j_uri=neo4j_uri, dump_directory='tests/2')
        nodes = load_test_nodes(cls._adcp.neo4j)
        relations = load_test_relations(cls._adcp.neo4j, nodes, 'rels_2')

    @classmethod
    def tearDownClass(cls):
        delete_test_nodes(cls._adcp.neo4j)

    def test_empty_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_11', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        self.assertIsNone(graph)

    def test_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_1', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')

        # 2 * GROUPS (11 + 11)
        self.assertEqual(order(graph), 22)
        # 2 * GROUPS (10 + 10) + link TEST_1 -> TEST_2 + link DENY
        self.assertEqual(size(graph), 22)

        self.assertEqual(len([x for x in graph.nodes() if x['NO_LINKS']]), 1)
        self.assertEqual(len([x for x in graph.relationships() if x['DENY']]), 1)


class TestAdcp3(TestAdcp.BaseTest):
    @classmethod
    def setUpClass(cls):
        neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
        cls._adcp = Adcp(neo4j_uri=neo4j_uri, dump_directory='tests/3')
        nodes = load_test_nodes(cls._adcp.neo4j)
        relations = load_test_relations(cls._adcp.neo4j, nodes, 'rels_3')

    @classmethod
    def tearDownClass(cls):
        delete_test_nodes(cls._adcp.neo4j)

    def test_empty_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_11', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        self.assertIsNone(graph)

    def test_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_1', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        # 4 * GROUPS (4 * 11)
        self.assertEqual(order(graph), 44)
        # 4 * GROUPS (4 * 10) + 3 Links + link DENY
        self.assertEqual(size(graph), 44)

        self.assertEqual(len([x for x in graph.nodes() if x['NO_LINKS']]), 1)
        self.assertEqual(len([x for x in graph.relationships() if x['DENY']]), 1)


class TestAdcp4(TestAdcp.BaseTest):
    @classmethod
    def setUpClass(cls):
        neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
        cls._adcp = Adcp(neo4j_uri=neo4j_uri, dump_directory='tests/4')
        nodes = load_test_nodes(cls._adcp.neo4j)
        relations = load_test_relations(cls._adcp.neo4j, nodes, 'rels_4')

    @classmethod
    def tearDownClass(cls):
        delete_test_nodes(cls._adcp.neo4j)

    def test_empty_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_11', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        self.assertIsNone(graph)

    def test_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_1', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        # 9 * GROUPS (9 * 11)
        self.assertEqual(order(graph), 99)
        # 9 * GROUPS (9 * 10) + 10 Links + link DENY
        self.assertEqual(size(graph), 100)

        self.assertEqual(len([x for x in graph.nodes() if x['NO_LINKS']]), 1)
        self.assertEqual(len([x for x in graph.relationships() if x['DENY']]), 1)


class TestAdcp5(TestAdcp.BaseTest):
    @classmethod
    def setUpClass(cls):
        neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
        cls._adcp = Adcp(neo4j_uri=neo4j_uri, dump_directory='tests/5')
        nodes = load_test_nodes(cls._adcp.neo4j)
        relations = load_test_relations(cls._adcp.neo4j, nodes, 'rels_5')

    @classmethod
    def tearDownClass(cls):
        delete_test_nodes(cls._adcp.neo4j)
        pass

    def test_empty_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_11', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        self.assertIsNone(graph)

    def test_control_path(self):
        base_node = [x for x in self._adcp.search('TEST_1', operator='=')][0]
        self.assertIsNotNone(base_node)

        graph = self._adcp.control_graph(base_node, 'to')
        # 9 * GROUPS (9 * 11)
        self.assertEqual(order(graph), 99)
        # 9 * GROUPS (9 * 10) + 10 Links + link DENY
        self.assertEqual(size(graph), 100)

        # Only 1 node with no link (TEST_28)
        self.assertEqual(len([x for x in graph.nodes() if x['NO_LINKS']]), 1)
        # 2 relations with DENY (TEST_27 and TEST_28)
        self.assertEqual(len([x for x in graph.relationships() if x['DENY']]), 2)


if __name__ == '__main__':
    unittest.main()
