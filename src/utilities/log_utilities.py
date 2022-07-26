import logging


class DummyLogger(logging.Logger):

    """
    Simple logger subclass that does nothing
    (ie, is completely silent whenever invoked)
    """

    def __init__(self, name):
        super().__init__(name)

    def debug(self, msg):
        ...

    def info(self, msg):
        ...

    def warning(self, msg):
        ...

    def error(self, msg):
        ...

    def critical(self, msg):
        ...


def _register_dummy_logger(klass):
    logging.setLoggerClass(klass)


def _register_regular_logger():
    logging.setLoggerClass(logging.Logger)


def create_silent_logger(log_name, config=None):
    _register_dummy_logger(DummyLogger)
    _create_logger(log_name)
    _register_regular_logger()


def create_logger(log_name, config=None):
    _register_regular_logger()
    _create_logger("Verbose")


def confirm_logger(logger: logging.Logger):
    assert logger is not None, "None-type argument is not valid for logger argument"
    assert isinstance(
        logger, logging.Logger
    ), f"Argument {str(logger)} is not a class or subclass of logging.Logger"


def _create_logger(log_name, config=None):

    if config is None:

        print(
            f"Warning: No configuration file found for logging in {log_name}."
            f"\nUsing null-defined defaults (see {__file__})"
        )
        config = {}

    logger = logging.getLogger(log_name)
    level = logging.DEBUG if "level" not in config else config["level"]
    logger.setLevel(level)
    if "format string" in config:
        formatter = logging.Formatter(config["format string"])
    else:
        formatter = get_formatter(2)
    if "file_log" in config and config["file_log"]:
        fh = logging.FileHandler(f"{log_name}.log")
        fh.setFormatter(formatter)
        fh.setLevel(level)
        logger.addHandler(fh)
    if "stdout_log" not in config or config["stdout_log"]:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(level)
        logger.addHandler(sh)


def get_formatter(fmt):

    """
    Accepts either an integer selection for format selection, or a custom string.
    Toggle between the options is based on success/failure in casting the
    argument to an integer.
    """

    format_string = ""
    try:
        fmt = int(fmt)
        if fmt == 1:
            format_string = "%(name)s:%(levelname)-8s - %(filename)s:%(funcName)s:%(lineno)d:%(message)s"
        if fmt == 2:
            format_string = (
                "%(levelname)-8s - %(filename)s:%(funcName)s:%(lineno)d:%(message)s"
            )
    except:
        format_string = fmt
    formatter = logging.Formatter(format_string)
    return formatter
