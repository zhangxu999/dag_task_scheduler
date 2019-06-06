import logging
logger = logging.getLogger(__name__)


def default_node_exception(task_info, name, operator, tb):
    logger.error(" {} {} {} ".format(task_info, name, operator))
    logger.error(tb)
