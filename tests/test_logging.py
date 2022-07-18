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
