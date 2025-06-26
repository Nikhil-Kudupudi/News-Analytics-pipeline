import logging 
import os
from datetime import datetime

# log files are created with month day,year,time
LOG_FILE=f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
#every file name gets added into  logs folder
logs_path=os.path.join(os.getcwd(),"logs")
# even though there is a file in that folder keep on adding hte new log files 
os.makedirs(logs_path,exist_ok=True)


LOG_FILE_PATH=os.path.join(logs_path,LOG_FILE)


# to override the  funcationality of logging we need to set the basic config

# refer documentation
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s ", # timestamp, level how the logging should be
    level=logging.INFO
    )
