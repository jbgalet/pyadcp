import logging
from csv import DictReader
from pathlib import Path
from timeit import default_timer as timer
from typing import Iterator, Union, Optional, Dict

from py2neo import Graph, Record, Subgraph, order, size, Relationship

TRANSLATION_TABLE = {
    'en': {
        'adm_dom': "cn=domain admins,",
        'adm_sch': "cn=schema admins,",
        'adm_ent': "cn=enterprise admins,",
        'adms': "cn=administrators,",
        'adm': "cn=administrator,",

        'dc': "cn=domain controllers,cn=users,dc=",
        'rodc': "cn=read-only domain controllers,cn=users,dc=",
        'cdc': "cn=cloneable domain controllers,cn=users,dc=",
        'erodc': "cn=enterprise read-only domain controllers,cn=users,dc=",

        'accop': "cn=account operators,cn=builtin,dc=",
        'srvop': "cn=server operators,cn=builtin,dc=",
        'backop': "cn=backup operators,cn=builtin,dc=",
        'printop': "cn=print operators,cn=builtin,dc=",
        'cryptop': "cn=cryptographic operators,cn=builtin,dc=",
        'netop': "cn=network configuration operators,cn=builtin,dc=",
        'axxop': "cn=access control assistance operators,cn=builtin,dc=",

        'dom_usr': "cn=domain users,cn=users,dc=",
        'dom_cmp': "cn=domain computers,cn=users,dc=",
        'dom_gue': "cn=domain guests,cn=users,dc=",
        'usr': "cn=users,cn=builtin,dc=",
        'guests': "cn=guests,cn=builtin,dc=",
        'guest': "cn=guest,cn=users,dc=",
        'prew2k': "cn=pre-windows 2000 compatible access,cn=builtin,dc=",
        'waac': "cn=windows authorization access group,cn=builtin,dc=",

        'certpub': "cn=cert publishers,cn=users,dc=",
        'gpoco': "cn=group policy creator owners,cn=users,dc=",
        'incftb': "cn=incoming forest trust builders,cn=builtin,dc=",
        'krbtgt': "cn=krbtgt,cn=users,dc=",
    },
    'fr': {
        'adm_dom': "cn=admins du domaine,",
        'adm_sch': "cn=adminstrateurs du schema,",
        'adm_ent': "cn=administrateurs de l.entreprise,",
        'adms': "cn=administrateurs,",
        'adm': "cn=administrator,",

        'dc': "cn=contr.leurs de domaine,cn=users,dc=",
        'rodc': "cn=contr.leurs de domaine en lecture seule,cn=users,dc=",
        'cdc': "cn=contr.leurs de domaine clonables,cn=users,dc=",
        'erodc': "cn=contr.leurs de domaine d.entreprise en lecture seule,cn=users,dc=",

        'accop': "cn=op.rateurs de compte,cn=builtin,dc=",
        'srvop': "cn=op.rateurs de serveur,cn=builtin,dc=",
        'backop': "cn=op.rateurs de sauvegarde,cn=builtin,dc=",
        'printop': "cn=op.rateurs d.impression,cn=builtin,dc=",
        'cryptop': "cn=op.rateurs de chiffrement,cn=builtin,dc=",
        'netop': "cn=op.rateurs de configuration r.seau,cn=builtin,dc=",
        'axxop': "cn=op.rateurs d.assistance de contr.le d.acc.s,cn=builtin,dc=",

        'dom_usr': "cn=utilisateurs du domaine,cn=users,dc=",
        'dom_cmp': "cn=ordinateurs du domaine,cn=users,dc=",
        'dom_gue': "cn=invit. du domaine,cn=users,dc=",
        'usr': "cn=utilisateurs,cn=builtin,dc=",
        'guests': "cn=invit.s,cn=builtin,dc=",
        'guest': "cn=invit.,cn=users,dc=",
        'prew2k': "cn=acc.s compatible pr.-windows 2000,cn=builtin,dc=",
        'waac': "cn=groupe d.acc.s d.autorisation windows,cn=builtin,dc=",

        'certpub': "cn=.diteurs de certificats,cn=users,dc=",
        'gpoco': "cn=propri.taires cr.ateurs de la strat.gie de groupe,cn=users,dc=",
        'incftb': "cn=g.n.rateurs d.approbations de for.t entrante,cn=builtin,dc=",
        'krbtgt': "cn=krbtgt,cn=users,dc=",
    }
}
MAX_DEPTH = 20
EXCHANGE_LABELS = [
    'RBAC_SET_MBX',
    'RBAC_ADD_MBXPERM',
    'RBAC_ADD_MBXFOLDERPERM',
    'RBAC_CONNECT_MBX',
    'RBAC_NEW_MBXEXPORTREQ',
]

TRANSITIVE_RELATIONS = [
    'GROUP_MEMBER',
    'PRIMARY_GROUP',
    'SID_HISTORY'
]

NON_ACE_RELATIONS = [
    'GROUP_MEMBER',
    'PRIMARY_GROUP',
    'SID_HISTORY',
    'CONTAINER_HIERARCHY',
    'GPLINK',
    'AD_OWNER',
    'SYSVOL_OWNER'
]


class Adcp(object):
    def __init__(self,
                 neo4j_uri: str,
                 dump_directory: Optional[Union[str, Path]] = None,
                 max_depth: int = MAX_DEPTH):
        self.logger = logging.getLogger('adcp')
        self.neo4j = Graph(neo4j_uri)

        self.__max_depth = max_depth

        if not dump_directory:
            self.__workdir = None
        else:
            self.__workdir = dump_directory if isinstance(dump_directory, Path) else Path(dump_directory)
        self.__denied_ace = []

    def is_workdir_valid(self):
        return self.__workdir and self.__workdir.exists() and (self.__workdir / 'Relations').exists()

    def id(self, name: str) -> int:
        result = self.neo4j.run(
            'MATCH (n) WHERE n.name = {name} RETURN id(n) as id',
            name=name)
        while result.forward():
            cur = result.current()
            return int(cur['id'])

    def get_node(self, id: int) -> Record:
        result = self.neo4j.run(
            'MATCH (n) WHERE id(n) = {id} RETURN n.name as name, id(n) as id',
            id=id)
        while result.forward():
            cur = result.current()
            return cur

    def search(self, name: str, lang: str = 'en', operator: str = 'STARTS WITH') -> Iterator[Record]:
        search_str = TRANSLATION_TABLE.get(lang, dict()).get(name, name)

        result = self.neo4j.run(
            'MATCH (n) WHERE n.name %s {name} RETURN DISTINCT n.name as name, id(n) as id' % operator,
            name=search_str)
        while result.forward():
            cur = result.current()
            yield cur

    def __get_rels(self, relations: Dict, source: str) -> Iterator:
        for target, types in relations.get(source, dict()).items():
            for typeid in types:
                yield dict(source=source, target=target, type=typeid)
                if typeid in TRANSITIVE_RELATIONS:
                    for rel in self.__get_rels(relations, target):
                        yield dict(source=source, target=rel['target'], type=rel['type'])

    def __denyace(self, graph: Subgraph, graph_target: str) -> Subgraph:
        if not self.__workdir:
            self.logger.warning('No denyace applied - dump directory not provided')
            return graph

        rels = dict()

        # Load DENY Ace from Relation folder
        if not self.__denied_ace:
            for deny_file in (self.__workdir / 'Relations').glob('*.deny.csv'):
                with deny_file.open('r', encoding='utf-16-le') as ifile:
                    self.logger.debug('Parsing denied ACE from %s', deny_file)
                    reader = DictReader(ifile)
                    for row in reader:
                        self.__denied_ace.append(row)
            self.logger.debug('Denied ACE : %d', len(self.__denied_ace))

        nodes = dict()

        # Step 1 : cut all direct relationships
        for edge in graph.relationships():
            source = edge.start_node()['name']
            nodes.setdefault(source, edge.start_node())

            target = edge.end_node()['name']
            nodes.setdefault(target, edge.end_node())

            denied = [x for x in self.__denied_ace
                      if x['dnMaster:START_ID'] == source
                      and x['dnSlave:END_ID'] == target
                      and x['keyword:TYPE'] == edge.type()]

            if len(denied) > 0:
                self.logger.debug('DENY edge (%s) --[%s]--> (%s)', source, edge.type(), target)
                edge['DENY'] = '1'

        # Step 2 : build relation map
        for edge in graph.relationships():
            if edge['denied']:
                continue
            source = edge.start_node()['name']
            target = edge.end_node()['name']
            rels.setdefault(source, dict())
            rels[source].setdefault(target, [])
            rels[source][target].append(edge.type())

        # Step 3 : map denied to relationships
        nolink_nodes = []
        for source in rels:
            path_count = 0
            denies = 0
            for rel in self.__get_rels(rels, source):
                if rel['target'] == graph_target:
                    path_count += 1
                denied = [x for x in self.__denied_ace
                          if x['dnMaster:START_ID'] == rel['source']
                          and x['dnSlave:END_ID'] == rel['target']
                          and x['keyword:TYPE'] == rel['type']]
                if len(denied) > 0:
                    path_count -= 1
                    denies += 1
                    deny_edge = Relationship(nodes[rel['source']], 'DENY_{}'.format(rel['type']), nodes[rel['target']],
                                             DENY=1)
                    graph |= deny_edge

            if path_count == 0 and denies > 0:
                nolink_nodes.append(source)

        # Step 4 : tag unlinked nodes
        for node in graph.nodes():
            if node['name'] in nolink_nodes:
                node['NO_LINKS'] = True

        return graph

    def __simplify_graph(self, graph: Subgraph) -> Subgraph:
        # List edges
        edge_dict = dict()
        edge_deny = dict()
        for edge in graph.relationships():
            source = edge.start_node()['name']
            target = edge.end_node()['name']
            edge_dict.setdefault(source + target, [])
            if not edge.type() in edge_dict[source + target]:
                edge_dict[source + target].append(edge.type())
            else:
                edge['__drop__'] = 1

            edge_deny.setdefault(source + target, False)
            edge_deny[source + target] |= bool(edge['DENY'])

        # Group edges
        new_edges = []
        for edge in graph.relationships():
            source = edge.start_node()['name']
            target = edge.end_node()['name']
            tags = edge_dict.get(source + target, [])
            if len(tags) > 1:
                edge['__drop__'] = 1
                if '__done__' not in tags:
                    new_rel = Relationship(edge.start_node(), ','.join(tags), edge.end_node())
                    if edge_deny.get(source + target):
                        new_rel['DENY'] = 1
                    edge_dict[source + target].append('__done__')

                    new_edges.append(new_rel)

        # Build the simplified graph
        new_graph = Subgraph(
            nodes=[x for x in graph.nodes()],
            relationships=[x for x in graph.relationships() if not x['__drop__']] + new_edges
        )

        return new_graph

    def control_graph(self, node: Record, direction: str) -> Optional[Subgraph]:
        graph = None

        labels = self.neo4j.relationship_types
        if '@' not in node['name']:
            labels = [x for x in labels if x not in EXCHANGE_LABELS]

        nodes = [node['id']]
        old_nodes = []
        dir_arrow = '-[:{}]->'.format('|'.join(labels)) if direction == 'from' else '<-[:{}]-'.format('|'.join(labels))

        # Step 1 : Find the control nodes (adjacent nodes) up to the defined depth
        start = timer()
        for i in range(0, self.__max_depth):
            query = 'MATCH path = (n)%s(m) WHERE id(n) IN {nodes} AND NOT id(m) IN {old_nodes} RETURN path, id(n) as n, id(m) as m' % dir_arrow

            result = self.neo4j.run(query, nodes=nodes, old_nodes=old_nodes)

            nodes = []
            count = 0
            while result.forward():
                count += 1
                cur = result.current()
                cur['path'].start_node()['id'] = cur['n']
                cur['path'].end_node()['id'] = cur['m']
                subgraph = cur.subgraph()
                if not cur['m'] in nodes:
                    nodes.append(cur['m'])

                if graph is None:
                    graph = subgraph
                else:
                    graph |= subgraph
            old_nodes += nodes

            self.logger.info('{} relations for {} nodes found at depth {}'.format(count, len(nodes), i + 1))

            if count == 0:
                break

        end = timer()
        self.logger.debug('[perf] Control nodes : %f', (end - start))

        if graph is None:
            self.logger.info('Empty graph')
            return graph

        self.logger.info('Initial graph: %d nodes - %d edges', order(graph), size(graph))

        # Step 2 : Apply DENY ACE
        start = timer()
        filtered_graph = self.__denyace(graph, node['name'])
        end = timer()
        self.logger.debug('[perf] Deny ACE : %f', (end - start))
        self.logger.info('Filtered graph: %d nodes - %d edges', order(filtered_graph), size(filtered_graph))

        # Step 3 : Simplify the graph
        start = timer()
        simplified_graph = self.__simplify_graph(filtered_graph)
        end = timer()
        self.logger.debug('[perf] Simplify : %f', (end - start))
        self.logger.info('Simplified graph: %d nodes - %d edges', order(simplified_graph), size(simplified_graph))

        return simplified_graph
