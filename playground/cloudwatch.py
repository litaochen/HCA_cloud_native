# playground for cloudwatch logs
# first install watchtower by:
#   pip install watchtower

import watchtower, logging

# variables
LOG_GROUP_NAME = 'hca-cloud-native'
LOG_STREAM_NAME = 'cellprofiler'

watchtower_config = {
    'log_group': LOG_GROUP_NAME,
    'stream_name': LOG_STREAM_NAME,
    'use_queues': True,
    'create_log_group': False
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

watchtowerlogger = watchtower.CloudWatchLogHandler(**watchtower_config)
watchtowerlogger.setFormatter(formatter)
logger.addHandler(watchtowerlogger)

logger.info("Hi")
logger.info(dict(foo="bar", details={}))