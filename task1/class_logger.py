from aiologger.formatters.base import Formatter


class Logger(Formatter):

    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%Y %H:%M'
        )
