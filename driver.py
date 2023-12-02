import os
import sys
from time import  perf_counter
import get_env_variables as gav
from create_spark import get_spark_object
from Validate import *
from ingest import load_files, display_df, df_count
import logging
import logging.config
from data_processing import *
from data_transformation import *
from extraction import *
from persist import *
logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_format, file_dir, header, inferSchema, file_dir2
    try:
        start_time = perf_counter()
        logging.info("i am in main method...")
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('object created...', spark)
        logging.info('Validating spark object.......')
        get_current_date(spark)

        # read olap directory
        for file in os.listdir(gav.src_olap):
            print("File is :", file)
            file_dir = gav.src_olap + '\\' + file
            print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info('reading file which is of > {}'.format(file_format))
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        logging.info("displaying file")
        #display_df(df_city, 'df_city')

        logging.info("here to validate the df")
        df_count(df_city, 'df_city')

        logging.info('checking for the files in the Fact...')

        for files in os.listdir(gav.src_oltp):
            print("Src Files::" + files)

            file_dir = gav.src_oltp + '\\' + files
            print(file_dir)

            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif files.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,inferSchema=inferSchema)

        logging.info('displaying the df_fact dataframe')
        #display_df(df_fact, 'df_fact')

        # validate::
        df_count(df_fact, 'df_fact')

        logging.info("implementing data processing methods....")
        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)

        display_df(df_city_sel, "df_city")
        display_df(df_presc_sel, "df_fact")

        logging.info("validating schema for dataframes...")
        print_schema(df_city_sel, "df_city_sel")
        print_schema(df_presc_sel, "df_presc_sel")

        logging.info("checking for null values in dataframes after procssing...")
        check_df = check_for_nulls(df_presc_sel, 'df_fact')
        display_df(check_df, "df_fact")

        logging.info("Data transformations executed...")
        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        logging.info("displaying df_report_1......")
        display_df(df_report_1, "data_report_1")

        logging.info("calling data report 2 method...")
        df_report_2 = data_report2(df_presc_sel)
        logging.info("displaying df_report_2......")
        display_df(df_report_2, "data_report_2")

        logging.info("extracting df_report_1 to output...")
        city_path = gav.city_path
        extract_files(df_report_1, 'json', city_path, 1, False, 'bzip2')

        logging.info("extracting df_report_2 to output...")
        presc_path = gav.presc_path
        extract_files(df_report_2, 'parquet', presc_path, 2, False, 'snappy')
        logging.info("extracting files completed...")

        logging.info("writing into hive table started...")
        data_hive_persist(spark=spark, df=df_report_1, dfname='df_city', partitionBy='state_name', mode='append')

        data_hive_persist_presc(spark=spark, df=df_report_2, dfname='df_presc', partitionBy='presc_state',
                                mode='append')
        logging.info("successfully written into hive...")

        logging.info("Now write {} into mysql...".format(df_report_1))
        persist_data_mysql(spark=spark,df=df_report_1,dfname='df_city',url=gav.mysqlurl,dbtable='city_df',mode='append',
                           user=gav.mysqluser,password=gav.mysqlpswd)
        logging.info("successfully data inserted into mysql table...")

        end_time = perf_counter()
        print(f"the process time {end_time - start_time: 0.2f} seconds()")

    except Exception as e:
        logging.error("An error occurred when calling main(). Please check the trace...", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
#    end_time=perf_counter()
 #   logging.info(f"the process time {end_time - start_time: 0.2f} seconds()")
    logging.info("Application done...")
