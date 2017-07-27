import json
import sys
from pathlib import Path

import click

try:
    import networkx as nx
except ImportError:
    nx = None  # For code warnings
    click.secho('[!] NetworkX is required, please install it (python3 -m pip install networkx)', fg='red')
    sys.exit(1)

OUTPUT_FORMATS = {
    'gexf': nx.write_gexf,
    'gml': nx.write_gml,
    'graphml': nx.write_graphml,
}


@click.command()
@click.option('--format',
              help='Format to use',
              default='list')
@click.argument('infile', type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=False, exists=True))
@click.argument('outdir', type=click.Path(file_okay=False, dir_okay=True, readable=True, writable=True, exists=False))
def convert(**kwargs):
    if kwargs['format'] == 'list':
        click.secho("Available formats:")
        for item in OUTPUT_FORMATS:
            click.secho('- {}'.format(item))
        return 0

    writer = OUTPUT_FORMATS.get(kwargs['format'])
    if not writer:
        click.secho('[!] Invalid format {format}'.format(**kwargs), fg='red')
        return 1

    infile = Path(kwargs['infile'])
    outdir = Path(kwargs['outdir'])

    with infile.open('r') as ifile:
        base = json.load(ifile)

    graph = nx.DiGraph()

    for idx, node in enumerate(base['nodes']):
        graph.add_node(idx, **node)

    for rel in base['links']:
        graph.add_edges_from([(rel['source'], rel['target'])], relations=','.join(rel['rels']))

    with (outdir / '{}.{}'.format(infile.stem, kwargs['format'])).open('wb') as ofile:
        writer(graph, ofile)

    outdir.mkdir(exist_ok=True)


if __name__ == '__main__':
    convert()
