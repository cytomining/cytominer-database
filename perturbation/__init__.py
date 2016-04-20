"""

"""

import pyximport
pyximport.install()

import click
import perturbation.database
import pkg_resources
import subprocess
import logging
import logging.config


def __version__(context, parameter, argument):
    if not argument or context.resilient_parsing:
        return

    click.echo('perturbation {}'.format(pkg_resources.get_distribution('perturbation').version))

    context.exit()


@click.command()
@click.argument('input', type=click.Path(dir_okay=True, exists=True, readable=True))
@click.help_option(help='')
@click.option('-o', '--output', type=click.Path(exists=False, file_okay=True, writable=True))
@click.option('-s', '--sqlfile', default='views.sql', type=click.Path(exists=True, file_okay=True, readable=True))
@click.option('-v', '--verbose', default=False, is_flag=True)
@click.option('-d', '--skipmunge', default=False, is_flag=True)
@click.option('-V', '--version', callback=__version__, expose_value=False, is_eager=True, is_flag=True)
def __main__(input, output, sqlfile, verbose, skipmunge):
    """

    :param input:
    :param output:
    :param verbose:

    :return:

    """

    import json

    with open("logging_config.json") as f:
        logging.config.dictConfig(json.load(f))

    logger = logging.getLogger(__name__)

    if not skipmunge:
        logger.debug('Calling munge')
        subprocess.call(['./munge.sh', input])
    else:
        logger.debug('Skipping munge')

    perturbation.database.seed(input=input, output=output, sqlfile=sqlfile, verbose=verbose)

    logger.debug('Finish')


if __name__ == '__main__':
    __main__()
