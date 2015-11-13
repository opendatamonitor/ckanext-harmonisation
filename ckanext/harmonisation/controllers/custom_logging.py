import logging


class Logger:
# logging.DEBUG
# logging.INFO
# logging.WARNING
# logging.ERROR
# logging.CRITICAL
    def __init__(self,logger_name,log_file,level=logging.INFO):
        l = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fileHandler = logging.FileHandler(log_file, mode='w')
        self.fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(self.fileHandler)
        # l.addHandler(streamHandler)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.fileHandler.close()

    def close(self, type, value, traceback):
        self.fileHandler.close()

