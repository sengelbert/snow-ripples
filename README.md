# snow-ripples
##### This is a simple experimental repo to stream XRPL data to Snowflake DB and splitting the XRPL data into tables using Snowflake Dynamic Tables. It was created for learning and fun though could be expanded upon.

### Prerequisites and Steps
1. Snowflake account ([Trials are Free!](https://signup.snowflake.com/))
2. Run "setup.sql" in Snowflake account. Make sure to set a super strong password
3. In config dir create config.yaml file (example below)
4. Alter config file to point to XRPL (testnet, prod, local/docker/podman). For more info on this maybe look [here first](https://github.com/WietseWind/docker-rippled) and then maybe [here](https://github.com/sengelbert/rippled-data)
5. Alter config file to point to your Snowflake account using your new super strong password
6. Install python libraries #Todo ./requirements.txt
7. Run "main.py"

### Example config/config.yaml
This example points to the XRPL testnet
```yaml
xrpl:
  paginate: True
  ssl: False # this can change dpending on your host configs
  # host: '127.0.0.1' # local
  host: 's.altnet.rippletest.net' # https://s.altnet.rippletest.net:51234/  #testnet
  # host: 'r.ripple.com' # https://r.ripple.com:51234/  # prod
  port: 51234 # this can change dpending on your host configs
  ledger_count: 1
  api_limit: 20000
  async: False
  batch: 100
snowflake:
  user: xrpl_streamer
  password: <your super strong pwd>
  account: <your snowflake account>
  warehouse: COMPUTE_WH
  database: xrpl
  schema: raw
  role: xrpl_stream
  table_name: raw_xrpl
  role: xrpl_stream
```