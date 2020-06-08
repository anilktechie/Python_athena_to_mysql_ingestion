import awswrangler as ws
import boto3
import os
from datetime import date, timedelta


def main(search_dt,infra_table,infra_version):

    dynamo_vars = boto3.resource('dynamodb').Table(infra_table).get_item(Key={"version":infra_version})['Item']['vars']['xx_your_info_xx']['xx_your_info_xx']

    print("Date filtered: "+ search_dt)

    ################### Variables
    athena_dbase= "your_athena_database"
    athena_table= "your_athena_table"
    ## Output info
    database_type= "mysql"
    host= dynamo_vars['dynamo_host_index']
    port= 3306
    aurora_database= dynamo_vars['dynamo_database_index']
    usuario= dynamo_vars['dynamo_user_index']
    senha= dynamo_vars['dynamo_password_index']
    aurora_table_name= dynamo_vars['dynamo_table-name_index']

    insert_query= f"select column x as y,...  from {athena_dbase}.{athena_table} where date_filter = '{search_dt}' group by ...;"

    print("Athena Database: "+ athena_dbase)
    print("Query executed on Athena: "+ insert_query)
    ##############################################################

    ## Execução da Query na Origem, retornando um DataFrame:
    print("Executing query...")
    # insert_df= ws.athena.read_sql_query(sql=insert_query,database=dbase,boto3_session=session)
    insert_df= ws.athena.read_sql_query(sql=insert_query,database=athena_dbase)

    # Estabelecimento de conexão com o MySql Aurora:
    engine= ws.db.get_engine(
                    db_type= database_type,
                    host= host,
                    port= port,
                    database= aurora_database,
                    user= usuario,
                    password= senha
                )

    # # In case you want to delete some data before inserting, you can use this sintax to do that
    # extra_query= f"DELETE FROM {aurora_table_name} WHERE search_date= '{search_dt}'"
    # engine.execute(extra_query)

    # Insert data into Mysql table
    print("Inserting data...")
    try:
        ws.db.to_sql(df=insert_df,con=engine,name=aurora_table_name,if_exists='append',index=False)
    except:
        print("No data to insert or communication error... ")
        pass

    print("End")
    
##############################################################################
##############################################################################

if __name__ == "__main__":

    infra_table = 'your_dynamodb_table'
    infra_version = 'your_dynamodb_version'

    reference_date = date.today()
    
    #Here there is a loop, which will read the past two dates: today-2, today-1

    for delta in range(2, 0, -1):
        raw_search_dt= reference_date - timedelta(delta)
        search_dt= raw_search_dt.strftime("%Y-%m-%d")

        print('***********************************************************')
        print(f'delta: {delta}')
        print('Executing query for day: '+search_dt)
        main(search_dt,infra_table,infra_version)

    exit()
