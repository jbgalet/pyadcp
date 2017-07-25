# query.py
Like query.rb, with some improvements

Early stage, tests are welcome

## Differences
- Bolt protocol support
- Neo4j authentication
- Faster

## Install
Only Python3 is supported
    python3 -m pip install -r requirements.txt

## Usage
    Usage: query.py [OPTIONS] COMMAND [ARGS]...

    Options:
      --lang TEXT          Language to use
      --maxdepth INTEGER   maximum length for control paths
      --neo4j TEXT         neo4j connection URI
      --workdir DIRECTORY  Root of ADCP dump directory
      -v, --verbose        Increase verbosity (add more to increase)
      -o, --options TEXT   Extra options to tweak behavior
      --noprompt           Disable application prompt (useful for batches)
      --help               Show this message and exit.

    Commands:
      graph
      list_aliases

The main command is graph and have works like `query.rb --quick`

    python3 query.py --workdir=[path_to dumps] --neo4j=bolt://user:password@localhost graph adm_dom to dump.json
