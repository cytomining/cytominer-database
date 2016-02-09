"""

$ celery -A perturbation.tasks worker -l info

"""

import celery

application = celery.Celery(main='tasks', backend='db+sqlite:///backend.sqlite', broker='sqla+sqlite:///broker.sqlite')


@application.task
def foo(data):
    """

    :param data:

    :return:

    """

    return data
