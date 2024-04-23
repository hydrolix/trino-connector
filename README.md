# Hydrolix Trino Connector

## Overview
This is a Trino plugin that allows Trino to access data stored in [Hydrolix](https://hydrolix.io/) 
clusters directly, without ETL. 

* Based on [connectors-core](https://github.com/hydrolix/connectors-core/).
* Currently targets version 435 of the Trino SPI. Other Trino versions, or Presto, might come later; Trino/Presto break 
  binary compatibility constantly! If you need a specific version, let us know and we can probably make it happen 
  quickly. 

## System Requirements

* Trino 435 
* Worker nodes must be recent (>=2022) x64 Linux. For local development on other machines, you can try Docker, 
  using [this configuration parameter](https://github.com/hydrolix/connectors-core?tab=readme-ov-file#configuration).  

## Building

1. Install [SBT](https://scala-sbt.org/) in whatever way makes sense for your OS
2. `git clone git@github.com:hydrolix/trino-connector.git hydrolix-trino-connector && cd hydrolix-trino-connector`
3. `sbt +assembly` will produce [./target/scala-2.13/hydrolix-trino-connector-assembly-0.1.0-SNAPSHOT.jar](target/scala-2.13/hydrolix-trino-connector-assembly-0.1.0-SNAPSHOT.jar)

## Running

1. Symlink the JAR that was built by the previous step into the right place in the trino-server directory:
    ```
    cd ~/dev/trino-server-435/plugin
    mkdir hydrolix && cd hydrolix
    ln -s ~/dev/hydrolix-trino-connector/target/scala-2.13/hydrolix-trino-connector-assembly-0.1.0-SNAPSHOT.jar .
    ```

2. Add a Hydrolix catalog configuration file to your Trino server:

    `cat ~/dev/trino-server-435/etc/catalog/hydrolix.properties`:
    ```
    connector.name=hydrolix
   
    jdbc_url=jdbc:clickhouse://my-hdx-cluster.net:8088/_local?ssl=true
    api_url=https://my-hdx-cluster.net/config/v1/
    
    username=hdx_user
    password=hdx_password
    
    ### For GCS:
    # cloud_cred_1=<base64(gzip(service account JSON key file))>
    
    ### For AWS:
    # cloud_cred_1=<AWS access key ID>
    # cloud_cred_2=<AWS secret key>  
   
    ### For local dev in Docker
    # turbine_cmd_docker=my_image_name
    ```
   
3. Run your Trino server:
    ```
    cd ~/dev/trino-server-435
    bin/launcher run
    ```
    
    Or, if you want to attach a debugger:
    ```
    cd ~/dev/trino-server-435
    bin/launcher \
        -J-Xdebug \
        -J-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 \
        run
    ```

4. Connect a Trino client and run a query!
   ```
   java -jar trino-cli-435-executable.jar --catalog=hydrolix --user=admin
   
   trino> select count(*), min(timestamp), max(timestamp) from hydro.logs where timestamp > now() - (interval '5' minute);
    _col0 |          _col1          |          _col2          
   -------+-------------------------+-------------------------
    29699 | 2024-01-11 21:55:00.061 | 2024-01-11 22:04:01.245 
   (1 row)
   
   Query 20240111_220408_00000_dctfe, FINISHED, 1 node
   Splits: 17 total, 17 done (100.00%)
   8.63 [29.7K rows, 261KB] [3.44K rows/s, 30.3KB/s] 
   ```

## Feature Set
See [connectors-core](https://github.com/hydrolix/connectors-core/?tab=readme-ov-file#feature-set).

## Release Notes

### 0.1.0

Initial release!
