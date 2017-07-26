import sys
import os

from py2neo import Graph, Relationship, Node


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
    return neo4j.run('MATCH (n) WHERE n.is_test_node DETACH DELETE n')


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


def main():
    neo4j_uri = os.environ.get('NEO4J_URI', 'bolt://localhost')
    neo4j = Graph(neo4j_uri)

    try:
        if sys.argv[1] == 'load':
            test_id = int(sys.argv[2])
            nodes = load_test_nodes(neo4j)
            relations = load_test_relations(neo4j, nodes, 'rels_{}'.format(test_id))
            print("Loaded test {} : {} nodes | {} relations".format(test_id, len(nodes), len(relations)))
        elif sys.argv[1] == 'clean':
            delete_test_nodes(neo4j)
            print("Test data removed")
        else:
            raise RuntimeError("Invalid arguments")
    except:
        print("Usage: load_test.py [load|clean] <test_id>")
        sys.exit(1)

if __name__ == '__main__':
    main()