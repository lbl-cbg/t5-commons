
from CSBID import CSBIDTask
from simple_scattering import SimpleScatteringTask
from mailin_SAXS import MailinSAXSTask
import datetime
import time
import logging

logging.basicConfig(filename= '/logs/'+str(datetime.date.today())+'.log',filemode='a')
logger = logging.getLogger("Logger") 
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
formatter = logging.Formatter(
    "{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
console_handler.setFormatter(formatter) 
logger.setLevel("DEBUG")




try:
    logger.info("JIRA Cronjob started...") 
    CSBID_task = CSBIDTask()
    update_response = CSBID_task.run()

    simple_scattering_task = SimpleScatteringTask()
    update_response = simple_scattering_task.run()

    mailin_SAXS_task = MailinSAXSTask()
    update_response = mailin_SAXS_task.run()
except Exception as e:
    logger.error("Error in jira cronjob...") 
    logger.exception(e) 
