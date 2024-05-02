import json
import datetime
import yaml
import snowflake.connector

from xrpl.models import LedgerData
from xrpl.clients import JsonRpcClient
from xrpl.ledger import get_latest_validated_ledger_sequence


def main():

    print(f"start time: {datetime.datetime.now()}")

    config_file = 'config/config.yaml'
    print(f"🛈 Attempting to read yaml file: {config_file}...")
    with open(f'{config_file}', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # set some variables
    call_count = 0
    marker_val = None
    paginate = config['xrpl']['paginate']
    ssl = config['xrpl']['ssl']
    host = config['xrpl']['host']
    port = config['xrpl']['port']
    ledger_count = config['xrpl']['ledger_count']
    api_limit = config['xrpl']['api_limit']

    table_name = config['snowflake']['table_name']
    database = config['snowflake']['database']
    schema = config['snowflake']['schema']
    role = config['snowflake']['role']
    warehouse = config['snowflake']['warehouse']

    # Snowflake Connection
    conn = snowflake.connector.connect(
        user=config['snowflake']['user'],
        password=config['snowflake']['password'],
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema']
    )

    # Create a cursor object
    cur = conn.cursor()

    cur.execute(f"USE ROLE {role}")
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {schema}")

    # create a network client
    is_ssl = 'https' if ssl else 'http'
    connection_string = f"{is_ssl}://{host}:{port}/"
    client = JsonRpcClient(connection_string)
    # https://s.altnet.rippletest.net:51234/  #testnet
    # https://r.ripple.com:51235/  # prod

    ledger = get_latest_validated_ledger_sequence(client)

    # loop through API results
    for i in range(ledger_count):
        # need to decrement for multiple ledger loops
        while paginate:
            if marker_val is None:
                ledger_result = client.request(LedgerData(ledger_index=ledger, limit=int(api_limit)))
            else:
                ledger_result = client.request(LedgerData(ledger_index=ledger, marker=marker_val, limit=int(api_limit)))

            ledger_result = ledger_result.result
            call_count += 1

            if "marker" in ledger_result:
                marker_val = ledger_result["marker"]
            else:
                marker_val = None
                paginate = False

            if call_count == config['xrpl']['batch']:
                print(f"{datetime.datetime.now()}: Ledger: {ledger}. "
                      f"APICall Count: {call_count}. "
                      f"Inserting into {table_name}...")

                snowflake_insert = f"INSERT INTO {database}.{schema}.{table_name}(json_data, host, ledger) SELECT PARSE_JSON('{json.dumps(ledger_result)}'), '{host}', '{ledger}';"
                if config['xrpl']['async']:
                    cur.execute_async(snowflake_insert)
                else:
                    cur.execute(snowflake_insert)

    ledger -= 1

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
