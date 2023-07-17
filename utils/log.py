import logging
import datetime


def get_logger():
    date = datetime.datetime.now().date()
    logFormatter = logging.Formatter("%(asctime)s [%(processName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger("INTRADAY")
    filename = "log/{}-app.log".format(date)
    fileHandler = logging.FileHandler(filename)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(logging.DEBUG)
    return rootLogger


logger_instance = get_logger()
