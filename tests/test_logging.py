import logging

import utilities as utils

# Confirm standard logging

std_logger_name = "TestLogger"
silent_logger_name = "SilentType"
restored_logger_name = "VerboseTest"

utils.create_logger("TestLogger")
standard_logger = logging.getLogger(std_logger_name)
# Register a silent-type logger
utils.register_dummy_logger(utils.DummyLogger)
utils.create_logger(silent_logger_name)
silent_logger = logging.getLogger(silent_logger_name)
# Restore the original logging `logger` class
utils.register_regular_logger()
utils.create_logger(restored_logger_name)
restored_logger = logging.getLogger(restored_logger_name)

standard_logger.info(f"Hello, I'm a normal logger")
silent_logger.info(f"You shouldn't hear this")
restored_logger.info(f"Working again! (From a new logger")

standard_logger.debug("The original logger is now debugging")
silent_logger.debug("In space, no one can hear you scream")
restored_logger.debug("Restored logger, now debugging!")

logger_list = [standard_logger, silent_logger, restored_logger]

def func_to_test_info_level(msg: str, loggers: list[logging.Logger]) -> None:
    if type(loggers) is logging.Logger:
        loggers = [loggers]
    else:
        assert (
            type(loggers) is list
        ), "Argument `loggers` must be either logging.Logger or list of [logging.Logger]"
        assert [type(l) is logging.Logger for l in loggers], \
            "All items in list `loggers` must be of type logging.Logger"
    for l in loggers:
        print(f"{'-'*20}{'Info Testing: Testing logger ' + l.name:^40}{'-'*20}")
        l.info(f"{l.name}: {msg}")
        print(f"{'-'*20}{l.name + ': Test complete':^40}{'-'*20}")

func_to_test_info_level("Testing function name inspection!", logger_list)