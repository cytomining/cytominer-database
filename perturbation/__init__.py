"""

"""

import click
import perturbation.database
import pkg_resources
import subprocess


def __version__(context, parameter, argument):
    if not argument or context.resilient_parsing:
        return

    click.echo('perturbation {}'.format(pkg_resources.get_distribution("perturbation").version))

    context.exit()


@click.command()
@click.argument('input', type=click.Path(dir_okay=True, exists=True, readable=True))
@click.help_option(help='')
@click.option('-o', '--output', type=click.Path(exists=False, file_okay=True, writable=True))
@click.option('-v', '--verbose', default=False, is_flag=True)
@click.option('-V', '--version', callback=__version__, expose_value=False, is_eager=True, is_flag=True)
@click.option('--workers', default=1, nargs=1, type=click.INT)
def __main__(input, output, verbose, workers):
    """

    :param input:
    :param output:
    :param verbose:
    :param workers:

    :return:

    """

    subprocess.run(['./munge.sh', input])

    perturbation.database.seed(input=input, output=output, verbose=verbose)


if __name__ == '__main__':
    __main__()
