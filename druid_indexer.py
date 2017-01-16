#!/usr/bin/env python
import argparse
import json 
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import time
import subprocess
import csv
import pprint
import getpass
import os,glob,re
import logging
import shutil


class ConfigFile: 
    
    top_folder=os.getenv("PWD")
   
    def __init__(self, Field_Names=['item','dataSource','dataSource','dimensionsSpec','metricsSpec','interval','intervals','segmentGranularity','queryGranularity','indexingURL']):
        self.Field_Names=Field_Names
        self._row_list=[]
        self._config_templ_json={}
    
    @classmethod                                      
    def create_folders(cls):
        print cls.top_folder
        
        if not os.path.exists('{0}/log'.format(cls.top_folder)):
            os.makedirs('{0}/log'.format(cls.top_folder))
        if not os.path.exists('{0}/config'.format(cls.top_folder)):
            os.makedirs('{0}/config'.format(cls.top_folder))
        if not os.path.exists('{0}/config/template'.format(cls.top_folder)):
            os.makedirs('{0}/config/template'.format(cls.top_folder))
        if not os.path.exists('{0}/tmp'.format(cls.top_folder)):
            os.makedirs('{0}/tmp'.format(cls.top_folder))
        if not os.path.exists('{0}/kafkatask'.format(cls.top_folder)):
            os.makedirs('{0}/kafkatask'.format(cls.top_folder))
        if not os.path.exists('{0}/pid'.format(cls.top_folder)):
            os.makedirs('{0}/pid'.format(cls.top_folder))

    def load_csv_file(self,logger,csv_file='{0}/config/template/config_file.csv'.format(top_folder)):
        try:
            fn=open(csv_file,"r")
            reader=csv.reader(fn)
            for row in reader:
                self._row_list.append(row)
        except IOError: 
            logger.error("File {0} does not appear to exist. Please pass existing file to argument 'csv_file' in load_templates_files method ".format(csv_file))
            #print "Error: File %s does not appear to exist. Please pass existing file to argument 'csv_file' in load_templates_files method "%csv_file
            return

    def create_reindexing_json_files(self,logger,config_templ_json='{0}/config/template/reindex_temp.json'.format(top_folder)):
        try:
            fn=open(config_templ_json,"r")
            self._config_templ_json=json.load(fn)
          #  print self._config_templ_json
        except IOError: 
            logger.error("File {0} does not appear to exist. Please pass existing file to argument 'config_templ_json' in load_templates_files method".format(config_templ_json))
            #print "Error: File %s does not appear to exist. Please pass existing file to argument 'config_templ_json' in load_templates_files method"%config_templ_json
            return
        
        json_indexurl_dict={}
        
        if not self._row_list:
            logger.error('csv config file not loaded, please load it with load_templates_files method')
            #print 'csv config file not loaded, please load it with load_templates_files method'
            return
        for item_row,row  in enumerate( self._row_list,start=0):
            field_source=None
            index_url=row[-1]
            for item,field in enumerate(row,start=0):
                if item==0: continue
                if item==1: field_source=field
                
                if 'dimensionsSpec_template' in field:
                    try:
                        fn=open(field,"r") 
                        dim_template= json.load(fn)
                        ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],dim_template,1)
                    except IOError: 
                        logger.error("File {0} does not appear to exist. Please pass existing file  to argument 'dim_template' in load_templates_files method  ".format(field))
                        #print "Error: File %s does not appear to exist. Please pass existing file  to argument 'dim_template' in load_templates_files method  "%field
                elif 'metricsSpec_template' in field:
                    try:
                        fn=open(field,"r") 
                        metric_template= json.load(fn)
                        ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],metric_template,1)
                    except IOError: 
                        logger.error("File {0} does not appear to exist. Please pass existing file to argument 'metric_template' in load_templates_files method".format(field))
                        #print "Error: File %s does not appear to exist. Please pass existing file to argument 'metric_template' in load_templates_files method"%field
                        return
                elif  self.Field_Names[item]=='interval' or self.Field_Names[item]=='intervals':
                    matchedObj =re.match(r'^P(\d+Y|Y)?(\d+M|M)?(\d+W|W)?(\d+D|D)?-P(\d+Y|Y)?(\d+M|M)?(\d+W|W)?(\d+D|D)?$',field,re.M|re.I)
                    if matchedObj:
                        
                        date1=matchedObj.groups()[:4]
                        date2=matchedObj.groups()[4:]
                       
                        date1=[ y if y else None  for  y in date1 ]
                        date2=[ y  if y else None  for  y in date2 ]
                        
                        DATE1= TODAY = date.today()
                        DATE2= TODAY = date.today()
                        
                        for x in date1:
                            if x is not None:
                                if "Y" in x : DATE1=DATE1-relativedelta(years=+int(x.strip('Y')))
                                elif "M" in x: DATE1=DATE1-relativedelta(months=+int(x.strip('M')))
                                elif "W" in x: DATE1=DATE1-relativedelta(weeks=+int(x.strip('W')))
                                elif "D" in x: DATE1=DATE1-relativedelta(days=+int(x.strip('D')))
                        for x in date2:
                             if x is not None:
                                if "Y" in x : DATE2=DATE2-relativedelta(years=+int(x.strip('Y')))
                                elif "M" in x: DATE2=DATE2-relativedelta(months=+int(x.strip('M')))
                                elif "W" in x: DATE2=DATE2-relativedelta(weeks=+int(x.strip('W')))
                                elif "D" in x: DATE2=DATE2-relativedelta(days=+int(x.strip('D')))
                            
                        date_='{0}/{1}'.format(DATE2.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z',DATE1.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z')
                        ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],date_,1)
                        if self.Field_Names[item]=='intervals':
                            ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],[date_],1)
                       
                    else:
                        logger.error('<<<  FieldName "Intervals" or "interval"  must be  in the format of "P<year>Y<month>M<week><day>D-P<year>Y<month>M<week><day>D   as in  P2M3D-P1Y4W ( 2months 3 days ago - 1 year 4 weeks ago)   >>>'.format(field))
                        #print '<<< You entered "{0}" as interval in csv file.FieldName "Intervals" must be yesterday or lastweek or lastmonth >>>'.format(field)
                        logger.error('{0}/tmp/{1}/config_reindex_{2}.json could not be created'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),item_row))
                        #print '{0}/tmp/{1}/config_reindex_{2}.json could not be created'.format(top_folder,date.today().strftime('%Y-%m-%d'),item_row)
                        break;

                elif item==2:
                    ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],field,2)
                else:
                    ConfigFile.replace_nd(self._config_templ_json,self.Field_Names[item],field,1)
            else :
                filename = '{0}/tmp/{1}/{2}/config_reindex_{3}.json'.format(os.getenv("PWD"),date.today().strftime('%Y-%m-%d'),os.getpid(),item_row)
                json_indexurl_dict[filename]=index_url
                if not os.path.exists(os.path.dirname(filename)):
                    try:
                        os.makedirs(os.path.dirname(filename))
                    except OSError as exc:
                        if exc.errno != errno.EEXIST:
                            raise
                with open(filename, "w") as outfile:
                    json.dump(self._config_templ_json,outfile, sort_keys = True, indent = 4,ensure_ascii=False)

        return json_indexurl_dict
  
    @classmethod
    def replace_nd(cls,adict, k, v, nd):
        if not hasattr(cls, "_flag"):
            cls._flag=0

        for key in adict.keys():
            if key == k :
                cls._flag += 1
                if cls._flag==nd:
                    adict[key] = v
                    return 
            elif type(adict[key]) is dict :
                ConfigFile.replace_nd(adict[key], k, v,nd)  
        
        if cls._flag==nd: 
            cls._flag=0
            return    

    @classmethod
    def find_nd(cls,v, k):
        found_list=[]
        if type(v) == type({}):
            for k1 in v:
                if k1 == k:
                    found_list.append(v[k1])
                else:
                    found_list.extend(cls.find_nd(v[k1], k))
	return found_list

def submit_task(tobeSubmitted_list,running_dict, finished_dict, max_task, check_period, json_indexurl_dict,logger):

    if len(tobeSubmitted_list)==0:return
    
    shrinked_list=tobeSubmitted_list[:max_task-len(running_dict)]
    
    if len(shrinked_list)!=0: 
        logger.info('To be submitted: {0} {1}'.format(len(shrinked_list),shrinked_list))
        #print 'To be submitted:', len(shrinked_list), shrinked_list

    for j in shrinked_list:
        
        url=json_indexurl_dict[j].replace('task','supervisor')
        val=subprocess.Popen('{0} {1}'.format(curl_get, url),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')
        list_ = re.findall(r'"(.*?)"', val)
        
        if len(list_) > 0:
            logger.error('There are some running kafka indexing tasks. Make sure that all kafka tasks are shutdown before continuing with reindexing \nExited')
            #print 'There are some running kafka indexing tasks. Make sure that all kafka tasks are shutdown before continuing with reindexing \nExited'
            return
      
        var=subprocess.Popen("curl --silent -X POST -H 'Content-Type: application/json' -d @{0} {1}".format(j,json_indexurl_dict[j]),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')
        if var is '' : 
            logger.warning('"{0}" could not be submitted. Druid is not running, skipping.'.format(j))
            #print '"{0}" could not be submitted. Druid is not running, skipping.'.format(j)
            tobeSubmitted_list.remove(j)
            continue
        
        list_=re.findall(r'"(.*?)"', var)
        fn=open(j,"r") 
        tmp= json.load(fn)
        fn.close
        filename = '{0}/pid/pid_{1}_{1}.pid'.format(ConfigFile.top_folder,ConfigFile.find_nd(tmp,'dataSource')[0])
        
        #print list_,ConfigFile.find_nd(tmp,'dataSource')[0]
        if os.path.exists(filename): 
            logger.warning('The task producing datasource "{0}" is already being run by another instance of the script right now. Skipping it'.format(ConfigFile.find_nd(tmp,'dataSource')[0]))
            #print 'The task producing datasource "{0}" is already being run by another instance of the script right now. Skipping it'.format(ConfigFile.find_nd(tmp,'dataSource')[0])
            tobeSubmitted_list.remove(j)
            continue
     
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        open(filename,'a').close()
        tobeSubmitted_list.remove(j)
        running_dict[j]=list_[1]
        logger.info("task associated with datasource '{0}' just submitted".format(ConfigFile.find_nd(tmp,'dataSource')[0]))
        
    while True:

        time.sleep(check_period)
        _finished_list=[]
       
        tmp_dict=dict(running_dict)
        
        for key,value in tmp_dict.iteritems():
            if int(subprocess.Popen('curl --silent -X GET  {0}{1}/status | grep -c FAILED'.format(json_indexurl_dict[key],tmp_dict[key]),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')):
                os.remove('{0}/pid/pid_{1}_{1}.pid'.format(ConfigFile.top_folder,tmp_dict[key].split('_')[1]))
                logger.error('task associated with the datasource named "{0}" failed, Please check {1} file'.format(tmp_dict[key].split('_')[1],key))
                #print 'task associated with the datasource named "{0}" failed, because it is not found'.format(tmp_dict[key].split('_')[1])
                del running_dict[key]
                #tmp_dict[key]=value
            val=int(subprocess.Popen('curl --silent -X GET  {0}{1}/status | grep -c SUCCESS'.format(json_indexurl_dict[key],tmp_dict[key]),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n'))
            if val==1:
                _finished_list.append(key)
        
        if len(_finished_list)!=0:
            for j in _finished_list:
                #print running_dict[j]
                liste=running_dict[j].split('_')
                #finished_list.append(j)
                finished_dict[j]=liste[1]
                os.remove('{0}/pid/pid_{1}_{1}.pid'.format(ConfigFile.top_folder,liste[1]))
                del running_dict[j]
                
        logger.info('# of running task: {0}'.format(len(running_dict)))
        #print '# of running task: ',len(running_dict)
        
        submit_task(tobeSubmitted_list,running_dict,finished_dict,max_task,check_period,json_indexurl_dict,logger)
        if len(running_dict)==0: break

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-10.12s] [%(levelname)-4.7s]  %(message)s")
rootLogger = logging.getLogger()
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='druidindexer')

    subparsers = parser.add_subparsers(help='Actions')

    # create the parser for the "reindex" command
    parser_a = subparsers.add_parser('reindex', help='Reindex datasources')
    parser_a.add_argument('--input_csv_file',type=str, action='store', dest='CONFILE_PATH', required=True,help='config file path for reindexing (required)')

    parser_a.add_argument('--check_period', type=int, action='store', dest='CHECK_PERIOD',default=60,help= 'time interval in minutes for checking # of running task (default 60  min)')

    parser_a.add_argument('--config_item_list', nargs='+', action='store', type=int,dest='CONF_ITEM_LIST',help= 'item numbers of configFile')
    parser_a.add_argument('--max_task', type=int, action='store',dest='MAX_TASK_REINDEX',required=True,help= 'number of concurrent tasks (required)' )

    parser_a.set_defaults(which='reindex')

    # create the parser for the "launch" command
    parser_b = subparsers.add_parser('launch', help='Launch kafka indexing tasks')

    parser_b.add_argument('--hostname_port',type=str, action='store', dest='HOSTNAME_PORT', required=True,help='Set the hostname and port  for druidindexer (required)')

    parser_b.add_argument('--all', action='store_true', default=False,help= 'Launch all kafka indexing tasks - associated json files locating under kafkatask directory')

    parser_b.add_argument('--file_name_list', nargs='+',action='store', type=str,dest='FILE_NAME_LIST',help= 'list of kafka indexing tasks - json file under kafkatask directory')

    parser_b.add_argument('--wait', type=int, action='store', dest='WAIT_TIME_KAFKA_INDEX', required=True,help= 'time interval in minutes between kafka indexing tasks')

    parser_b.set_defaults(which='launch')

# create the parser for the "stop" command

    parser_c = subparsers.add_parser('shutdown', help='Shutdown  kafka indexing tasks')

    parser_c.add_argument('--hostname_port',type=str, action='store', dest='HOSTNAME_PORT', required=True,help='Set the hostname and port for druidindexer (required)')


    parser_c.add_argument('--all', action='store_true', default=False,help= 'Shutdown all running kafka indexing tasks')


    parser_c.add_argument('--data_source_list',nargs='+',action='store',type=str, dest='DATA_SOURCE_LIST',help='Shutdown specified kafka indexing tasks')
    parser_c.set_defaults(which='shutdown')
    
    parser_d= subparsers.add_parser('display_all', help='Display all running kafka indexing tasks')

    parser_d.add_argument('--hostname_port',type=str, action='store', dest='HOSTNAME_PORT', required=True,help='Set the hostname and port for druidindexer (required)')

    parser_d.set_defaults(which='display_all')

    args=parser.parse_args()

    curl_get='curl --silent -X GET'
    curl_post='curl --silent  -X POST'
    curl_post_json="curl --silent -X POST -H 'Content-Type: application/json'"

    print args
    conf = ConfigFile()


    if args.which is 'reindex':

        filename = '{0}/log/{1}/{2}/reindex.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),os.getpid())
        #filename = '{0}/log/{1}/reindex_{2}.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),time.strftime("%H.%M.%S"))
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)
        
        rootLogger.setLevel(logging.INFO)
        
        if not os.path.exists(ConfigFile.top_folder+'/config/template'):
            rootLogger.info("Creating folders")
            #print "Creating folders"
            conf.create_folders()
            rootLogger.info('Please first, put the "reindex_temp.json", "metricsSpec_template.json" and "dimensionsSpec_template.json" under {0}/config/template , then run the script again'.format(ConfigFile.top_folder))
            #print 'Please first, put the "reindex_temp.json", "metricsSpec_template.json" and "dimensionsSpec_template.json" under {0}/config/template , then run the script again'.format(ConfigFile.top_folder)
            exit()
        conf.load_csv_file(rootLogger, csv_file=args.CONFILE_PATH)
        json_indexurl_dict=conf.create_reindexing_json_files(rootLogger)
    
        #conf.setup()
        #exit()

        list_files=glob.glob('{0}/tmp/{1}/{2}/*.json'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),os.getpid()))
        list_files.sort(key=lambda var:[int(x) if x.isdigit() else x for x in re.findall(r'[^0-9]|[0-9]+', var)])


        TobeSubmitted_list=[]
        if args.CONF_ITEM_LIST is not None:
            TobeSubmitted_list= map(lambda var:'{0}{1}.json'.format(re.split(r'\d+\.json',list_files[0])[0],var),  args.CONF_ITEM_LIST)
            NotTobeSubmitted_list= filter(lambda var: not os.path.exists(var),TobeSubmitted_list)
            if len(NotTobeSubmitted_list)!=0: 
                rootLogger.warning(NotTobeSubmitted_list +' dont exit, will not be submitted')
                #print NotTobeSubmitted_list, ' dont exit, will not be submitted'

            TobeSubmitted_list= filter(lambda var: os.path.exists(var),TobeSubmitted_list)
        else: TobeSubmitted_list=list_files

        if TobeSubmitted_list==[] :
            rootLogger.error('No existing json file under tmp directory\nExited')
            #print 'No existing json file under tmp directory\nExited'
            exit()
            
        Running_dict={}
        Finished_dict={}
        submit_task(TobeSubmitted_list,Running_dict,Finished_dict,args.MAX_TASK_REINDEX,args.CHECK_PERIOD, json_indexurl_dict,rootLogger)
        list_datasources=Finished_dict.values()
        list_datasources.sort(key=lambda var:[int(x) if x.isdigit() else x for x in re.findall(r'[^0-9]|[0-9]+', var)])
        rootLogger.info('Reindexed datasources :  {0}'.format(list_datasources))
        

    elif args.which is 'launch':
        filename = '{0}/log/{1}/{2}/launch.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),os.getpid())
        #filename = '{0}/log/{1}/reindex_{2}.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),time.strftime("%H.%M.%S"))
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)
        
        rootLogger.setLevel(logging.INFO)
        
        if not os.path.exists(ConfigFile.top_folder+'/kafkatask'):
            rootLogger.info("Creating folders")
            conf.create_folders()
            rootLogger.warning('Please first, put some kafka indexing task json file under {0}/kafkatask then run the script again'.format(ConfigFile.top_folder))
            
        hostname_port_url='{0}/druid/indexer/v1/supervisor/'.format(args.HOSTNAME_PORT)

        if not os.path.exists('{0}/kafkatask'.format(ConfigFile.top_folder)):
            os.makedirs('{0}/kafkatask'.format(ConfigFile.top_folder))     
            TobeSubmitted_list=[]
        
        if  args.FILE_NAME_LIST is None and args.all==False :
                rootLogger.error('druidindexer launch: error: argument --file_name_list or flag --all is required\nExited')
                #print 'druidindexer launch: error: argument --file_name_list or flag --all is required\nExited'
                exit()

        elif args.FILE_NAME_LIST is None and args.all==True:

            TobeSubmitted_list=glob.glob('{0}/kafkatask/*.json'.format(ConfigFile.top_folder))
            TobeSubmitted_list.sort(key=lambda var:[int(x) if x.isdigit() else x for x in re.findall(r'[^0-9]|[0-9]+', var)])

        elif args.FILE_NAME_LIST is not None and args.all==False:
            TobeSubmitted_list= map(lambda var:'{0}/kafkatask/{1}'.format(ConfigFile.top_folder,var) ,args.FILE_NAME_LIST)
            TobeSubmitted_list.sort(key=lambda var:[int(x) if x.isdigit() else x for x in re.findall(r'[^0-9]|[0-9]+', var)])
            NotTobeSubmitted_list= filter(lambda var: not os.path.exists(var),TobeSubmitted_list)
            if len(NotTobeSubmitted_list)!=0: print NotTobeSubmitted_list, 'tasks dont exist, will not be submitted'
            TobeSubmitted_list= filter(lambda var: os.path.exists(var),TobeSubmitted_list)

        else : 
            rootLogger.error('argument --file_name and flag --all not allowed to be assigned together\nExited')
            #print 'argument --file_name and flag --all not allowed to be assigned together\nExited'
            exit()

        if TobeSubmitted_list==[] :
            rootLogger.error('No existing json file under kafkatask directory\nExited')
            #print 'No existing json file under kafkatask directory\nExited'
            exit()
        
        task_id=[]
        for f in TobeSubmitted_list:
            val=subprocess.Popen("{0} -d @{1} {2}".format(curl_post_json,f,hostname_port_url),shell=True,stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0]
            if val=='':
                rootLogger.error('Druid is not running\nExited')
                #print 'Druid is not running\nExited'
                exit()
            diction=json.loads(val)
            
            task_id.append(diction["id"])
            time.sleep(args.WAIT_TIME_KAFKA_INDEX)
                
        time.sleep(20)
        for i,f in enumerate( TobeSubmitted_list):
            val=subprocess.Popen('{0} http://localhost:8090/druid/indexer/v1/supervisor/{1}/status'.format(curl_get,task_id[i]),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0]
            diction=json.loads(val)
            #print diction
            active_task=diction["payload"]["activeTasks"]
            #print active_task
            if  active_task==[]: 
                rootLogger.warning('"{0}" not submitted, might not have permission to start kafka indexing service '.format(f))
                #print '"{0}" not submitted, might not have permission to start kafka indexing service '.format(f) 
            else:
                rootLogger.info('"{0}" submitted'.format(f))
                #print '"{0}" submitted'.format(f)
            

    elif args.which is 'shutdown':

        hostname_port_url='{0}/druid/indexer/v1/supervisor/'.format(args.HOSTNAME_PORT)
        filename = '{0}/log/{1}/{2}/shutdown.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),os.getpid())

        #filename = '{0}/log/{1}/reindex_{2}.log'.format(ConfigFile.top_folder,date.today().strftime('%Y-%m-%d'),time.strftime("%H.%M.%S"))
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
                
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)
        
        rootLogger.setLevel(logging.INFO)

        TobeShutdown_list=[]
        if  args.DATA_SOURCE_LIST is None and args.all==False :
            rootLogger.error('druidindexer launch: error: argument --data_source_list or flag --all is required\nExited')
            #print 'druidindexer launch: error: argument --data_source_list or flag --all is required\nExited'
            exit()

        elif args.DATA_SOURCE_LIST is None and args.all==True:
            val=subprocess.Popen('{0} {1}'.format(curl_get,hostname_port_url),shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE).communicate()[0].strip('\n')
            TobeShutdown_list = re.findall(r'"(.*?)"', val)

        elif args.DATA_SOURCE_LIST is not None and args.all==False:

            NotTobeShutdown_list=filter(lambda var: int(subprocess.Popen('{0} {1}{2} | grep -c error '.format(curl_get, hostname_port_url,var),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')),args.DATA_SOURCE_LIST)

        
            if len(NotTobeShutdown_list)!=0: 
                rootLogger.info(NotTobeShutdown_list+' datasource/(s) dont exist, will not be shutdown')
                #print NotTobeShutdown_list, 'datasource/(s) dont exist, will not be shutdown'
            TobeShutdown_list= filter(lambda var: not int(subprocess.Popen('{0} {1}{2} | grep -c error '.format(curl_get, hostname_port_url,var),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')),args.DATA_SOURCE_LIST)

        else: 
            rootLogger.error('argument --data_source_list  and flag --all not allowed to be assigned together\nExited')
            #print 'argument --data_source_list  and flag --all not allowed to be assigned together\nExited'
            exit()    

        if TobeShutdown_list==[]: 
            rootLogger.info('There is no kafka indexing task to be shutdown')
            #print 'There is no kafka indexing task to be shutdown'
        else:
            for j in TobeShutdown_list:

                p=subprocess.Popen('{0} {1}{2}/shutdown  > /dev/null 2 >&1'.format(curl_post,hostname_port_url,j),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
                rootLogger.info('kafka task associated with datasource "{0}" will be shutdown'.format(j))
                #print 'kafka task associated with datasource "{0}" will be shutdown'.format(j)

    elif args.which is 'display_all':

        hostname_port_url='{0}/druid/indexer/v1/supervisor/'.format(args.HOSTNAME_PORT)

        val=subprocess.Popen('{0} {1}'.format(curl_get, hostname_port_url),shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE).communicate()[0].strip('\n')
        Running_list = re.findall(r'"(.*?)"', val)
        print 'Running list of kafka indexing task:',Running_list


