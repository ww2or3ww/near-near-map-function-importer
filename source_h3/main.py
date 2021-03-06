import sys
sys.dont_write_bytecode = True

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
_consoleHandler = logging.StreamHandler(sys.stdout)
_consoleHandler.setLevel(logging.INFO)
_simpleFormatter = logging.Formatter(
    fmt='%(levelname)-5s %(funcName)-20s %(lineno)4s: %(message)s'
)
_consoleHandler.setFormatter(_simpleFormatter)
logger.addHandler(_consoleHandler)

import lambda_function

def main():
    try:
        event = {}
        ret = lambda_function.lambda_handler(event, None)
        logger.info("event = {0}".format(event))
        logger.info("result = {0}".format(ret))

    except Exception as e:
        logger.exception(e)

main()