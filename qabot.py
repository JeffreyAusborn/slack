
from __future__ import  print_function

import sys, os
# sys.path.append('/home/arvind/data-analytics-tools/')
from slackclient import SlackClient
from time import gmtime, strftime
import MySQLdb
from sshtunnel import SSHTunnelForwarder

from time import sleep
import re
import json


cmds= ['versions', None]
sources= ['merchant_group_app', 'default_app', 'suggestion', 'merchant_app', 'default_rom']


def is_private_chat(sc, channel, user):
    # pm_channel = json.loads(sc.api_call('im.open', user=user))
    pm_channel = sc.api_call('im.open', user=user)
    
    if('error' in pm_channel):
        return False
    pm_channel_id = pm_channel["channel"]["id"]
    # Private message
    if pm_channel_id == channel:
        return True
    return False

def is_valid_channel(sc, channel):
    # In drabot_talk_test channel
    if (channel == 'C9MJ4EEH4'):
        return True
    return False


WHITE_LISTED_USERS = [] 

def update_whitelist():
    global WHITE_LISTED_USERS
    with open("users.txt", "r") as userfile:
        WHITE_LISTED_USERS= userfile.read().splitlines()
 

#def open_and_initiate_pm(sc, query, user):
#    # Open a PM to the user
#    pm_channel = json.loads(sc.api_call('im.open', user=user))
#    pm_channel_id = pm_channel["channel"]["id"]
#    print(pm_channel_id)
#    # Log an error to the calling channel
#    if pm_channel_id is None:
#        sc.rtm_send_message(m['channel'], 'Error opening direct chat')
#        return None
#
#    if user not in WHITE_LISTED_USERS:
#        sc.rtm_send_message(pm_channel_id, 'Access to cs-bot is currently limited')
#        return None
#
#    try:
#        # All other messages will be sent to the PM
#        sc.rtm_send_message(pm_channel_id, 'working on ' + query + '... ')
#    except:
#        return None
#
#    return pm_channel_id


def help(sc,channel):
    # with open("users.txt", "a") as userfile:
    #     userfile.write('')
    # update_whitelist()
    with open("help.txt", "r") as helpfile:
        helpText= helpfile.read()
    sc.rtm_send_message(channel, helpText)


def fetch_merchant_app_versions(sc, user, channel, text, input_list, envmnt):
    sc.rtm_send_message(channel, 'working on ' + text + '... ')
    try:
        with SSHTunnelForwarder((''+envmnt+'.dev.clover.com', 22), ssh_password='9ijnBHU*9ijnBHU*', ssh_username='jeffrey.ausborn', remote_bind_address=('127.0.0.1', 3306)) as server:
            conn = MySQLdb.connect(host='127.0.0.1', port=server.local_bind_port, user='root', passwd='', db='meta')
            cursor = conn.cursor()     # get the cursor

            print (input_list)
            query="SELECT mg.name, m.name, mg.uuid FROM merchant_group mg join merchant_groups mgs on mgs.merchant_group_id = mg.id join merchant m on m.id = mgs.merchant_id where m.uuid = "+input_list+" and mgs.deleted_time is NULL"
            cursor.execute(query)
            something = cursor.fetchall()
            builderString = ''
            
            mgName = ''
            mgArray = []
            mgArray1 = []
            mName = ''
            for item in something:
                #print (item)
                countit = 0
                for each in item:
                    if countit == 0:
                        mgName += "`"+str(each)+"` \n"
                        mgArray1.append(each)
                    elif countit == 1:
                        mName = str(each)
                    elif countit == 2:
                        mgArray.append(each)
                    countit+=1
                    #builderString+=str(each)+' '
                #builderString+='\n'
            if len(mgArray) == 0:
                clean_and_send(sc, channel, "Responding to: " + text + "\n\nYour merchant is not a part of any merchant_groups. Getting defaults...")
                fetch_default_app_versions(sc, user, channel, 'Get Default for '+str(input_list), envmnt)
                return
            else:    
                clean_and_send(sc, channel, "Responding to: " + text + "\n\nYour merchant `"+mName.replace('\n', '')+"` is in group(s) \n"+mgName+" \n")

            
            #query="SELECT da.name as developer_app_name,av.version_name  as android_version_name FROM merchant_group mg join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id join android_version av on av.id = mgav.android_version_id join developer_app da on da.id = av.developer_app_id where mg.uuid = "+input_list+" GROUP BY android_version_name ORDER BY developer_app_name"


            for i in range(0, len(mgArray)):
                #print ("foo")
                query="""
    SELECT 
	da.name as developer_app_name,
	av.version_name  as android_version_name
   	FROM
    merchant_group mg
	join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	join android_version av on av.id = mgav.android_version_id
	join developer_app da on da.id = av.developer_app_id
	where
	mg.uuid = '"""+mgArray[i]+"""'
    and da.package_name like '%com.clover%'
    ORDER BY developer_app_name"""
                print ('query2')
                builderString=''
                cursor.execute(query)   
                something = cursor.fetchall()
                builderString = ''
                for item in something:
                    for each in item:
                        builderString+=str(each)+' '
                    builderString+='\n'
                if len(builderString) < 2:
                    builderString = 'No Apps'
                clean_and_send(sc, channel, "\n*"+mgArray1[i]+" Apps*:\n```" + builderString + "```")



                
            #query="""SELECT 
	#da.name as developer_app_name,
	#av.version_name  as android_version_name
   	#FROM
    #merchant_group mg
	#join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	#join android_version av on av.id = mgav.android_version_id
	#join developer_app da on da.id = av.developer_app_id
	#join merchant_groups mgs on mgs.merchant_group_id = mg.id
	#join merchant m on m.id = mgs.merchant_id
	#where
	#m.uuid = """+input_list+"""
    #and da.package_name like '%com.clover%'
    #and mgs.deleted_time is NULL
    #ORDER BY developer_app_name"""
    
    

            query1="""SELECT
	distinct(da.name) as developer_app_name,
	av.version_name as android_version_name
    FROM 
    merchant_group mg
	join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	join android_version av on av.id = mgav.android_version_id
	join developer_app da on da.id = av.developer_app_id and da.android_version_id = av.id
	join merchant_groups mgs on mgs.merchant_group_id = mg.id
	 join merchant m on m.id = mgs.merchant_id
	where
	 da.package_name like '%com.clover%'
	 and av.id > 0
	and da.package_name not in (SELECT 
	da.name as developer_app_name
   	FROM
    merchant_group mg
	 join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	 join android_version av on av.id = mgav.android_version_id
	 join developer_app da on da.id = av.developer_app_id
	 join merchant_groups mgs on mgs.merchant_group_id = mg.id
	 join merchant m on m.id = mgs.merchant_id
	 where
	m.uuid = """+input_list+"""
	and da.package_name like '%com.clover%'
	and mgs.deleted_time is NULL)"""




            print ('query1')
            cursor.execute(query1)   
            something = cursor.fetchall()
            builderString1 = ''
            for item in something:
                #print (item)
                for each in item:
                    builderString1+=str(each)+' '
                builderString1+='\n'

                
            clean_and_send(sc, channel, "\n*Default Apps*:\n ```"+builderString1+"```")
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        sc.rtm_send_message(channel, 'Couldnt connect to that environment')
        print (e)


def fetch_merchantgroup_app_versions(sc, user, channel, text, input_list, envmnt):
    sc.rtm_send_message(channel, 'working on ' + text + '... ')
    try:
        with SSHTunnelForwarder((''+envmnt+'.dev.clover.com', 22), ssh_password='9ijnBHU*9ijnBHU*', ssh_username='jeffrey.ausborn', remote_bind_address=('127.0.0.1', 3306)) as server:
            conn = MySQLdb.connect(host='127.0.0.1', port=server.local_bind_port, user='root', passwd='', db='meta')
            cursor = conn.cursor()     # get the cursor

            print (input_list)
            query="SELECT distinct(mg.name) as merchant_group_name FROM merchant_group mg join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id join android_version av on av.id = mgav.android_version_id join developer_app da on da.id = av.developer_app_id where mg.uuid = "+input_list+""
            cursor.execute(query)
            something = cursor.fetchall()
            builderString2 = ''
            for item in something:
                for each in item:
                    builderString2+=str(each)+' '
                builderString2+='\n'
            clean_and_send(sc, channel, "Responding to: " + text + "\n\nYour merchant group is: `"+builderString2.replace('\n', '')+"` \n\n")

            
            #query="SELECT da.name as developer_app_name,av.version_name  as android_version_name FROM merchant_group mg join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id join android_version av on av.id = mgav.android_version_id join developer_app da on da.id = av.developer_app_id where mg.uuid = "+input_list+" GROUP BY android_version_name ORDER BY developer_app_name"


            query="""
    SELECT 
	da.name as developer_app_name,
	av.version_name  as android_version_name
   	FROM
    merchant_group mg
	join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	join android_version av on av.id = mgav.android_version_id
	join developer_app da on da.id = av.developer_app_id
	where
	mg.uuid = """+input_list+"""
    and da.package_name like '%com.clover%'
    ORDER BY developer_app_name"""
    
    #UNION
            
            query1="""SELECT
	da.name as developer_app_name,
	av.version_name as android_version_name
    FROM 
    merchant_group mg
	join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	join android_version av on av.id = mgav.android_version_id
	join developer_app da on da.id = av.developer_app_id and da.android_version_id = av.id
	where
	av.id > 0
	and da.package_name like '%com.clover%'
	and da.name not in (SELECT 
	da.name as developer_app_name
   	FROM
    merchant_group mg
	join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
	join android_version av on av.id = mgav.android_version_id
	join developer_app da on da.id = av.developer_app_id
	where
	mg.uuid = """+input_list+""")
GROUP BY developer_app_name ORDER BY developer_app_name"""


            print ('query')
            cursor.execute(query)   
            something = cursor.fetchall()
            builderString = ''
            for item in something:
                for each in item:
                    builderString+=str(each)+' '
                builderString+='\n'
            print ('query1')
            cursor.execute(query1)   
            something = cursor.fetchall()
            builderString1 = ''
            for item in something:
                for each in item:
                    builderString1+=str(each)+' '
                builderString1+='\n'

                
            clean_and_send(sc, channel, "\n*Merchant Group Apps*:\n```" + builderString + "```\n*Default Apps*:\n```"+builderString1+"```")
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        sc.rtm_send_message(channel, 'Couldnt connect to that environment')
        print (e)


def fetch_default_rom_versions(sc, user, channel, text, envmnt):
    sc.rtm_send_message(channel, 'working on ' + text + '... ')
    try:
        with SSHTunnelForwarder((''+envmnt+'.dev.clover.com', 22), ssh_password='9ijnBHU*9ijnBHU*', ssh_username='jeffrey.ausborn', remote_bind_address=('127.0.0.1', 3306)) as server:
            conn = MySQLdb.connect(host='127.0.0.1', port=server.local_bind_port, user='root', passwd='', db='meta')
            cursor = conn.cursor()     # get the cursor
            cursor1 = conn.cursor()
            
            query = """select id from device_type where id >1
                    """

            cursor.execute(query)   
            something = cursor.fetchall()
            builderString = ''
            for item in something:
                query1 = """select dt.name, r.version_name, r.build_type from rom r join device_type dt on dt.id = r.device_type_id
    where device_type_id = """+str(item[0])+""" and published = 1
    and version = (select version from rom where device_type_id = """+str(item[0])+"""
    and published = 1 order by id desc limit 1)
                        """

                cursor1.execute(query1)   
                something1 = cursor1.fetchall()
                
                for item1 in something1:
                    
                    for each in item1:
                        builderString+=str(each)+'\t'
                    builderString+='\n'

                
            clean_and_send(sc, channel, "Responding to: " + text + "\n\n*Default Roms*:\n```" + builderString + "```")

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        sc.rtm_send_message(channel, 'Couldnt connect to that environment')
        print (e)





def fetch_default_app_versions(sc, user, channel, text, envmnt):
    sc.rtm_send_message(channel, 'working on ' + text + '... ')
    try:
        with SSHTunnelForwarder((''+envmnt+'.dev.clover.com', 22), ssh_password='9ijnBHU*9ijnBHU*', ssh_username='jeffrey.ausborn', remote_bind_address=('127.0.0.1', 3306)) as server:
            conn = MySQLdb.connect(host='127.0.0.1', port=server.local_bind_port, user='root', passwd='', db='meta')
            cursor = conn.cursor()     # get the cursor

            query = """SELECT
                        da.name as developer_app_name,
                        av.version_name as android_version_name
                        FROM 
                        merchant_group mg
                        join merchant_group_android_version mgav on mgav.merchant_group_id = mg.id
                        join android_version av on av.id = mgav.android_version_id
                        join developer_app da on da.id = av.developer_app_id and da.android_version_id = av.id
                        where
                        av.id > 0
                        and da.package_name like '%com.clover%'
                        GROUP BY developer_app_name ORDER BY developer_app_name
                        """

            cursor.execute(query)   
            something = cursor.fetchall()
            builderString = ''
            for item in something:
                for each in item:
                    builderString+=str(each)+' '
                builderString+='\n'
            clean_and_send(sc, channel, "Responding to: " + text + "\n\n*Default Apps*:\n```" + builderString + "```")

    except (MySQLdb.Error, MySQLdb.Warning) as e:
        sc.rtm_send_message(channel, 'Couldnt connect to that environment')
        print (e)

    
    # query="select concat( 'https://stg1.dev.clover.com/internal/crashes/crash-groups/', lpad( rg.uuid,LENGTH(rg.uuid),0) ) as crash_group, ur.app_package_name, ur.report_type, ur.merchant_id, m.name from unified_report_group rg join unified_report ur on rg.id = ur.unified_report_group_id join merchant m on m.id = ur.merchant_id where ur.created_time >= now() - interval 10 day and ur.app_package_name like '%com.clover%' group by rg.uuid order by ur.created_time DESC;"


def clean_and_send(sc,channel,text):
    if len(text) > 4000:
        sc.rtm_send_message(channel,text[:3976] + "```\n...results truncated")
    else:
        sc.rtm_send_message(channel,text)
        
def logactivity(user,channel,message):
    time= strftime("%Y-%m-%d %H:%M:%S", gmtime())
    message= re.sub('[^0-9a-zA-Z ,.-]','',message)
    with open('qabot.log','a') as f:
        f.write('%s - user: %s from channel: %s asked: %s\n' %(time, user, channel, message))


def parse_request(message):
    message= re.sub(r'<@U1FM2HV6X>:?','',message)
    message= re.sub(r', +',',',message)
    message.strip()
    pieces_dirty= message.split()

    pieces = [item.strip() for item in pieces_dirty]
    if (len(pieces) == 1):
        return pieces[0],None,None,None
    elif (len(pieces) == 2):
        return pieces[0],pieces[1],None,None
    elif (len(pieces) == 4):
        return pieces[0],pieces[1],pieces[2],"'" + pieces[3].replace(",","','") + "'"
    elif (len(pieces) == 3):
        return pieces[0],pieces[1],None,"'" + pieces[2].replace(",","','") + "'"
    elif pieces[0] == 'suggestion':
        return pieces[0],None,None, message[10:]
    else: 
        return None,None,None,None

def permission_control(sc, user,channel, source,cmd):
    if user not in WHITE_LISTED_USERS and source != 'devices':
        sc.rtm_send_message(channel, 'User not permitted to use source and/or command')
        return False
    return True

def main():
    # token = configs.load_key("~/.clover/chatbots/drabot.cfg", "token")  # found at https://api.slack.com/#auth)
    sc = SlackClient('xoxb-326113131872-cpvKbexytHIpiZ3ebW6EkbWR')
    # update_whitelist()

    if sc.rtm_connect():
        while True:
            messages = sc.rtm_read()
            for m in messages:
                if ('type' in m) and ('message' == m['type']) and ('text' in m) and ('user' in m):
                    text = m['text'].strip()
                    user = m['user']
                    channel = m['channel']
                    print (user)
                    print (channel)
                    print (text)

                    # Check if message is in pm or the cs_cases room
                    if not (is_private_chat(sc, channel, user) or is_valid_channel(sc, channel)):
                        continue
                    source,cmd,option,input_list = parse_request(text)
                    #stupid level bad option TODO: Make part of validity check for each source
                    # if (option is not None and re.match('\d+',option) is None and re.match('\d\d\d\d-\d?\d-\d?\d',option) is None and re.match('^C\d{3}[a-zA-Z]{2}\d{8}$',option) is None):
                    #     continue 
                    print (source,cmd, option,input_list)
                    #check if user is permitted
                    

                    #is help request?
                    if (source == 'help' and cmd is None):
                        help(sc, channel)
                        continue   
                   
                    #is talking to DRABOT
                    if (source not in sources or cmd not in cmds):
                        #sc.rtm_send_message(channel, 'Not using the correct format, please type help to see the list of commands')
                        continue
                    logactivity(user,channel,text)
                    #user has permission to source/command?
                    # if not permission_control(sc,user,channel,source,cmd):
                    #     continue      
                    #check input list
                    if input_list is not None:
                        if (input_list.count(',') > 7):
                            sc.rtm_send_message(channel, 'Please restrict searches to 8 or less elements in the list')  
                            continue
                    
               
                    # Check if it starts with a key word, execute correct function if it does
                    #check the input list is good for merchants and overrides TODO: Make restrictive for source
                    if input_list is not None:
                        if (input_list is not None and re.match('\'(([a-zA-Z0-9]{13,14}|[0-9]+)\',?)+',input_list) is None) and source != 'default_app' and source != 'suggestion' and source != 'default_rom':
                            sc.rtm_send_message(channel, 'Sorry, you have entered an invalid list of uuids or MIDs')
                            continue
                        
                        if (source == 'merchant_group_app'):
                            print ("inside merchant_group_app")
                            if (option is None):
                                if (cmd == 'versions'):
                                    print ("inside version")
                                    # fetch_overrides_salesforcereport(sc, user, channel,text, input_list)
                                    fetch_merchantgroup_app_versions(sc, user, channel,text, input_list, 'stg1')
                                    continue
                            elif (option is not None):
                                if (cmd == 'versions'):
                                    print ("inside version")
                                    # fetch_overrides_salesforcereport(sc, user, channel,text, input_list)
                                    fetch_merchantgroup_app_versions(sc, user, channel,text, input_list, option)
                                    continue
                        elif (source == 'merchant_app'):
                            print ("inside merchant_app")
                            if (option is None):
                                if (cmd == 'versions'):
                                    print ("inside version")
                                    # fetch_overrides_salesforcereport(sc, user, channel,text, input_list)
                                    fetch_merchant_app_versions(sc, user, channel,text, input_list, 'stg1')
                                    continue
                            elif (option is not None):
                                if (cmd == 'versions'):
                                    print ("inside version")
                                    # fetch_overrides_salesforcereport(sc, user, channel,text, input_list)
                                    fetch_merchant_app_versions(sc, user, channel,text, input_list, option)
                                    continue
                        elif (source == 'default_app'):
                            fetch_default_app_versions(sc, user, channel,text, input_list.replace("'",""))
                        elif (source == 'default_rom'):
                            fetch_default_rom_versions(sc, user, channel,text, input_list.replace("'",""))    
                        elif (source == 'suggestion'):
                            with open('suggestions.txt', 'a') as sug:
                                sug.write("\n"+input_list+"\n")
                            sc.rtm_send_message(channel, 'Your suggestion \n```'+input_list+'```\n was saved and will be looked at as soon as possible.')
                            sc.rtm_send_message('G9LTQ4C12', 'A suggestion was added \n```'+input_list+'```\n')
                    elif (input_list is None) and (source == 'merchant_group_app'):
                        sc.rtm_send_message(channel, 'Please add a merchant_group UUID')

                    else:
                        if (source == 'default_app'):
                            print ("inside default_app")
                            if (cmd == 'versions'):
                                print ("inside version")
                                fetch_default_app_versions(sc, user, channel,text, 'stg1')
                                continue 


                    # if (source == 'merchant'):
                    #     if (cmd == 'transactions'):
                    #         if (re.match('^\'[a-zA-Z0-9]{13}\'$',input_list) is not None):
                    #             fetch_merchant_transactions(sc,user,channel,text,input_list)
                    #     if (cmd == 'devicetrans'):
                    #         if (re.match('^\'[a-zA-Z0-9]{13}\'$',input_list) is not None and re.match('^C\d{3}[a-zA-Z]{2}\d{8}$',option) is not None):
                    #             fetch_merchant_devicetrans(sc,user,channel,text,option,input_list)
                    # if (source == 'devices'):
                    #     if (cmd == 'settings'):
                    #         fetch_devices_settings(sc,user,channel,text,input_list)


            sleep(1)
    else:
        print("Connection Failed, invalid token?")


if __name__ == "__main__":
    main()
