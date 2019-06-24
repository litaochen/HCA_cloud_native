# playground for cloudwatch logs
# first install watchtower by:
#   pip install watchtower

import watchtower, logging

# variables
LOG_GROUP_NAME = 'hca-cloud-native'
LOG_STREAM_NAME = 'cellprofiler'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

watchtowerlogger = watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=LOG_STREAM_NAME, create_log_group=False)
logger.addHandler(watchtowerlogger)

logger.info("Hi")
logger.info(dict(foo="bar", details={}))