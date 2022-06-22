import logging

from celery import Celery

from scielo_classic_website.migration import _controller
from dsm import configuration


app = Celery('tasks',
             backend=configuration.CLASSIC_WEBSITE_MIGRATION_CELERY_RESULT_BACKEND_URL,
             broker=configuration.CLASSIC_WEBSITE_MIGRATION_CELERY_BROKER_URL)

LOGGER = logging.getLogger(__name__)


def _handle_result(task_name, result, get_result):
    if get_result:
        return result.get()


###########################################
EXAMPLE_QUEUE = 'migr_default'


def example(data, get_result=None):
    res = task_example.apply_async(
        queue=EXAMPLE_QUEUE,
        args=(data, ),
    )
    return _handle_result("task example", res, get_result)


@app.task()
def task_example(data):
    return {"task": "example", "result": "done", "data": data}

###########################################

def create_mininum_record_in_isis_doc(pid, isis_updated_date, get_result=None):
    res = task_create_mininum_record_in_isis_doc.apply_async(
        queue=configuration.CLASSIC_WEBSITE_MIGRATION_QUEUE_DEFAULT,
        args=(pid, isis_updated_date, ),
    )
    return _handle_result("task create_mininum_record_in_isis_doc", res, get_result)


@app.task(name="create_mininum_record_in_isis_doc")
def task_create_mininum_record_in_isis_doc(pid, isis_updated_date):
    return _controller.create_mininum_record_in_isis_doc(pid, isis_updated_date)

