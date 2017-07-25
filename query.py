import json
import logging
import sys
from timeit import default_timer as timer

import click

from adcp import Adcp, TRANSLATION_TABLE


@click.group()
@click.pass_context
@click.option('--lang',
              help='Language to use',
              default='en')
@click.option('--maxdepth',
              help='maximum length for control paths',
              type=int,
              default=20)
@click.option('--neo4j',
              envvar='NEO4J_URI',
              help='neo4j connection URI',
              default='bolt://localhost')
@click.option('--workdir',
              help='Root of ADCP dump directory',
              type=click.Path(file_okay=False, dir_okay=True, readable=True, writable=False, exists=True))
@click.option('-v', '--verbose',
              help='Increase verbosity (add more to increase)',
              count=True)
@click.option('-o', '--options',
              help='Extra options to tweak behavior',
              default='+deny')
@click.option('--noprompt',
              help='Disable application prompt (useful for batches)',
              is_flag=True,
              default=False)
def cli(ctx, **kwargs):
    click.secho('===== ADCP =====')

    logging.getLogger('neo4j.bolt').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    if kwargs['verbose'] == 1:
        logging.basicConfig(level=logging.INFO)
    elif kwargs['verbose'] == 2:
        logging.basicConfig(level=logging.DEBUG)
    elif kwargs['verbose'] == 3:
        # Full DEBUG
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('neo4j.bolt').setLevel(logging.DEBUG)
        logging.getLogger('httpstream').setLevel(logging.DEBUG)

    ctx.obj['noprompt'] = kwargs['noprompt']
    ctx.obj['options'] = dict()
    for option in kwargs['options'].split(','):
        if option[0] not in ['+', '-']:
            click.secho('[!] Invalid option ({})'.format(option), fg='yellow')
        else:
            ctx.obj['options'][option[1:]] = (option[0] == '+')

    if not kwargs['lang'] in TRANSLATION_TABLE:
        click.secho('[!] Invalid language ({lang}) - using default'.format(**kwargs), fg='yellow')
        ctx.obj['lang'] = 'en'
    else:
        ctx.obj['lang'] = kwargs['lang']

    try:
        adcp = Adcp(
            neo4j_uri=kwargs['neo4j'],
            dump_directory=kwargs['workdir'],
            max_depth=kwargs['maxdepth'])
        ctx.obj['adcp'] = adcp
    except Exception as e:
        click.secho('[x] Exception: {} : {}'.format(type(e).__name__, e), fg='red')
        sys.exit(1)

    click.secho('[+] Neo4j: {neo4j}'.format(**kwargs), fg='green')

    if adcp.is_workdir_valid():
        click.secho('[+] Dump Path: {workdir}'.format(**kwargs), fg='green')
    else:
        click.secho('[!] Dump Path Invalid ({workdir})'.format(**kwargs), fg='yellow')
        click.secho('[!] Deny ACE will not be processed')

    click.secho('[+] Language: {lang}'.format(**ctx.obj), fg='green')
    click.secho('[+] Max Depth: {maxdepth}'.format(**kwargs), fg='green')


@cli.command(help='')
@click.pass_context
@click.argument('search')
@click.argument('direction')
@click.argument('outfile', type=click.File(mode='w'))
def graph(ctx, search, direction, outfile):
    adcp = ctx.obj['adcp']  # type: Adcp

    matches = [x for x in adcp.search(search)]

    if len(matches) == 0:
        click.secho('[x] Node not found ({})'.format(search), fg='red')
        return 1
    elif len(matches) == 1:
        root_item = matches[0]
    else:
        if ctx.obj['noprompt']:
            click.secho('[!] Multiple items and prompt disabled, exiting', fg='red')
            return 1
        click.secho('[!] Multiple choices:', fg='yellow')
        for indx, item in enumerate(matches):
            click.echo('[{:4}] {}'.format(indx, item['name']))
        value = click.prompt('Choice: ', type=int)
        if value >= len(matches):
            click.secho('[!] Bad value', fg='red')
            return 1
        else:
            root_item = matches[value]

    click.secho('[+] Building ControlGraph using root item {}'.format(root_item), fg='green')

    start = timer()
    control_graph = adcp.control_graph(root_item, direction)
    end = timer()

    click.secho('[+] Build time: {} seconds'.format(end - start), fg='green')

    if not control_graph:
        click.secho('[!] Empty graph', fg='yellow')
        return

    nodes = []
    edges = []

    node_indx = dict()

    click.secho('[+] Start Export', fg='green')
    for indx, node in enumerate(control_graph.nodes()):
        nodes.append(dict(
            id=node['id'],
            name=node['name'],
            shortname=node['name'].split(',')[0],
            type=[x for x in node.labels()][0],
        ))
        node_indx[node['name']] = indx

    for edge in control_graph.relationships():
        rels = edge.type().split(',')

        if ctx.obj['options'].get('deny'):
            if edge['DENY']:
                rels.append('DENY')  # For graph coloration - A better solution may be possible here
        else:
            for rel in [x for x in rels]:
                tmp = rel.split('_', maxsplit=1)
                if tmp[0] == 'DENY':
                    rels.remove(rel)
                    rels.remove(tmp[1])

        edges.append(dict(
            source=node_indx[edge.start_node()['name']],
            target=node_indx[edge.end_node()['name']],
            rels=rels
        ))

    outfile.write(json.dumps(dict(nodes=nodes, links=edges)))
    click.secho('[+] ControlGraph written to {}'.format(outfile.name), fg='green')


@cli.command()
@click.pass_context
def list_aliases(ctx):
    lang = ctx.obj['lang']
    for key, value in TRANSLATION_TABLE[lang].items():
        click.secho('{}\t{}'.format(key, value))


if __name__ == '__main__':
    cli(obj=dict())