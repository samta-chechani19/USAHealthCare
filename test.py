import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logging.info("i am in main method...")

logging.error("An error occurred when calling main(). Please check the trace...")