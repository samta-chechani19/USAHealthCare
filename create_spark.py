from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Create_spark')

def get_spark_object(envn, appName):
    try:
        logger.info("get_spark_object method started...")
        if envn == 'DEV':
            master = 'local[*]'
        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        # create Spark Session
        spark_conf = SparkConf()
        spark_conf.set("spark.app.name", appName)
        spark_conf.set("spark.master", master)
        spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport()\
            .config('spark.driver.extraclasspath','mysql-connector-java-8.0.29.jar')\
            .getOrCreate()

        return spark
    except Exception as exp:
        logger.error("An error occurred in get_spark_object...", str(exp))
        raise
    else:
        logger.info("Spark object created...")


## testing purpose added
#spark = get_spark_object(get_env_variables.envn, get_env_variables.appName)
#print('object created...', spark)
