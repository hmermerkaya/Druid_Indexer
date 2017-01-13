# Druid_Indexer
Druid_Indexer  was written in  python for the purpose of performing following actions

Actions	Description
launch 	Launch kafka indexing tasks
shutdown 	Shutdown kafka indexing tasks
display_all 	Display all running kafka indexing tasks
reindex 	Reindex datasources
 ./druid_indexer.py -h  to see the help menu (lines above)

launch 

./druid_indexer.py launch -h  to see the all the arguments and usage of action ‘launch’ below

usage: druidindexer launch [-h] --hostname_port HOSTNAME_PORT [--all]
                           [--file_name_list FILE_NAME_LIST [FILE_NAME_LIST ...]]
                           --wait WAIT_TIME_KAFKA_INDEX

optional arguments:
  -h, --help            show this help message and exit
  --hostname_port HOSTNAME_PORT   Set the hostname and port for druidindexer (required)
  --all                 Launch all kafka indexing tasks - associated json files locating under kafkatask directory
  --file_name_list FILE_NAME_LIST [FILE_NAME_LIST …]  list of kafka indexing task - json files under kafkatask
                        directory
  --wait WAIT_TIME_KAFKA_INDEX    time interval in minutes between kafka indexing tasks


The sample usage of script with launch action is as follows 

./druid_indexer.py launch --hostname_port localhost:8090  --wait 3   --all

In the first run of the script with the action of launch  it creates  the following subfolders structure under working directory if not exist,  and then exits the script
	/log
	/config
		/template
	/tmp
            /kafkatask
            /pid

After exiting, make sure that you put  some  kafka indexing task json files under kafkatask directory  then run the script again
 

shutdown
./druid_indexer.py shutdown -h  to see the all the arguments and usage of action ‘shutdown’ below

optional arguments:
  -h, --help            show this help message and exit
  --hostname_port  HOSTNAME_PORT    Set the hostname and port for druidindexer (required)
  --all                 Shutdown all running kafka indexing tasks
  --data_source_list DATA_SOURCE_LIST [DATA_SOURCE_LIST …]  Shutdown specified kafka indexing tasks

sample usage:
./druid_indexer.py shutdown --hostname_port localhost:8090 --all



display_all
sample usage:
./druid_indexer.py  display_all --hostname_port localhost:8090

reindex 
./druid_indexer.py reindex -h  to see the all the arguments and usage of action ‘shutdown’ below

optional arguments:
  -h, --help            show this help message and exit
  --input_csv_file CONFILE_PATH   config file path for reindexing (required)
  --check_period CHECK_PERIOD time interval in minutes for checking # of running task  (default 60  min)
  --config_item_list CONF_ITEM_LIST [CONF_ITEM_LIST ...]   item numbers of configFile
  --max_task MAX_TASK_REINDEX    number of concurrent tasks (required)

sample usage:
./druid_indexer.py reindex --input_csv_file /home/kafka/druidindexer/config/template/config_file.csv --max_task 2 --check_period 3

In the first run of the script with the action of reindex,  it creates  the subfolder structure given above  if not exist,  and then exits the script
After exiting, make sure that you first put the "reindex_temp.json", "metricsSpec_template.json" and "dimensionsSpec_template.json" under config/template , then run the script again

