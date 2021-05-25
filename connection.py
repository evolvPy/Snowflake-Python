from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, exc
import pandas as pd
import logging,sys, os
from datetime import datetime

def snow_connector(accoutName, userName, userPassword, databaseName, schemaName, warehouseName, roleName):
    try:
        engine = create_engine(URL(
            account = accoutName, #'<account_name>.<region_id>.<cloud>' (e.g. 'xy789.east-us-2.aws')
            user = userName,
            password = userPassword,
            database = databaseName,
            schema = schemaName,
            warehouse = warehouseName,
            role= roleName
        ))
        connection = engine.connect()
        return engine, connection
    except exc.SQLAlchemyError as e:
        print("Engine Creation Module")
        print(e)
        sys.exit(1)
    except exc.DBAPIError  as e:
        print("Engine Creation Module")
        print(e)
        sys.exit(1)



def insertData(fileName,conn,tableName):
    try:
        logging.info('Data loading for the table '+ tableName +' started at - ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        df = pd.read_csv(fileName,header=0)
        # maximum number of expressions in a list exceeded, expected at most 16,384,
        df.to_sql( name= tableName , con = conn, if_exists='replace', index = False, chunksize=16000)
        logging.info('Data loading for the table '+ tableName +' finished at - ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        query = 'select count(*) AS TBL_COUNT from ' + tableName +';'
        df = pd.read_sql(query, conn)
        return str(f"{(df.loc[0,'tbl_count']):,}")
    except exc.SQLAlchemyError as e:
        print("Engine Creation Module")
        print(e)
        sys.exit(2)
    except exc.DBAPIError  as e:
        print("Engine Creation Module")
        print(e)
        sys.exit(2)



def main():
    try:
        logFileName = r"C:\Snowflake-Python\log\logfile.txt"
        if os.path.exists(logFileName):
            os.remove(logFileName)
            logging.basicConfig(filename= logFileName, level=logging.INFO) ## Other logging modes can also be used - logging.DEBUG
        logging.info('The job started at - ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        dataFileName = r"C:\Snowflake-Python\data\zip_code_database.csv"
        tableName = 'python_connect'
        engine, conn = snow_connector('hg69887.us-east-2.aws', 'xxxxx', 'xxxxx', 'DEMO_DB', 'public', 'compute_wh', 'SYSADMIN')
        logging.info('Connection to Snowflake established at - ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        rowCount = insertData(dataFileName,conn,tableName)
        logging.info('Number of rows inserted in the table  '+ tableName +' - ' + rowCount + '   .... ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
        logging.info('The job ended at - ' + str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
    except OSError as err:
        print("OS error: {0}".format(err))
        sys.exit(0)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        sys.exit(0)
    finally:
        conn.close()
        engine.dispose()



if __name__ == "__main__":
    main()
    

