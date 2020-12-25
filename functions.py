import requests 
import pandas as pd
from datetime import datetime as dt
import time
import datetime
import smtplib
#import sys
import logging
import os
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

pd.options.mode.chained_assignment = None  # default='warn'
url = "https://-----------------.bitrix24.ru/rest/266/aw2t2filzlzoqsgk/"

# Настраиваем логирование.
logger = logging.getLogger()

# Время ожидания между запросами, в сек.
wait = 0.6

# Период в днях для загрузки данных частями.
# Для чего: если из Битрикса загружать активности за последние 180 дней к примеру, то получим Bad Gateway 502. Поэтому была реализована загрузка частями.
daysLoadPeriod = 15

# Функция проверки возможности обращения к API Битрикс24.
# Чтобы не было слишком много запросов делаем следующее:
# записываем текущее время в миллисекундах в файл, при старте работы какой-либо долгой фукнции мы проверяем наличие файла, если файла нет, то создаём свой и работаем
# если файл есть, то ждём 1 секунду и проверяем снова. Если время в файле более чем 20 минут, то файл принудительно удаляем и выполняем функцию.
def check(action='check'):
    freeze = 2 # ожидание в секундах
    maxWait =  12 # максимальное ожидание в минутах
    filepath = '/home/vafanasyev/venv/api.lock'
    
    i = 0 # Сколько ждать очереди, затем принудительно выполнять.
    if action == 'check':
        while os.path.exists(filepath):
            logger.info('Ожидание освобождения очереди.')
            
            file = open(filepath, "r")
            tmp = float(file.read())
            file.close()
            
            tmp = dt.fromtimestamp(tmp)
            # Принудительно продолжаем, если файл давно существует и удаляем его.
            if (dt.now()-tmp).total_seconds()/60 > maxWait*10:
                logger.info('В файле lock дата сохранена более ' + str(maxWait*10) + ' мин. назад, принудительно продолжаем и удаляем файл.')
                return 'start'
            # Принудительно продолжаем, если прошло более maxWait минут, но файл не трогаем.
            elif (dt.now()-tmp).total_seconds()/60 > maxWait:
                logger.info('В файле lock дата сохранена более ' + str(maxWait) + ' мин. назад, принудительно продолжаем.')
                return 'startForced'
            
            i += 1
            if (i*freeze)/60 > maxWait:
                logger.info('Ожидали более ' + str(maxWait) + ' мин, принудительно продолжаем.')
                return 'startForced'
            
            time.sleep(freeze)
        
        now = str(dt.timestamp(dt.now()))
        file = open(filepath, 'w')
        file.write(now)
        file.close()
        return 'start'
    
    # Если после прошлого запуска вернули start, то удаляем файл, т.е. освобождаем очередь.
    elif action == 'start':
        os.remove(filepath)
        return 0
    else:
        return 0
        
def send_mail(send_from, send_to, subject, message, files=[],
              server='smtp.office365.com', port=587, username='---------------', password='------------',
              use_tls=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    #msg['To'] = ','.join(send_to)
    #msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject


    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

# Функция загрузки активностей по дате
def getActivities(days,USERS=[],changedDate='NO'):
    # USERS - это DataFrame с данными сотрудников.
    # Если в нём имеются сотрудники, то загружаем активности только по ним.
    if len(USERS) > 0:
        logger.info('Загружаем список активностей по ' + str(len(USERS)) + ' сотр.')
        users = USERS['ID'].unique()
    else:
        logger.info('Загружаем список активностей. Без учёта сотрудников.')
        users = ['EMPTY']
    
    start = dt.now()
    
    # method
    method_name = "crm.activity.list"
    
    # Адрес api метода для запроса get 
    url_param = url + method_name
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []
    
    checkResult = check()
    i = 0
    
    for userID in users:
        daysTmp = days
    
        while daysTmp > 0:
            dateFrom = dt.timestamp(dt.now()) - (daysTmp) * 24 * 60 * 60
            dateFrom = dt.fromtimestamp(dateFrom)
            # Формат даты в Битриксе: 2017-01-21T18:12:15+03:00
            dateFromB24 = dateFrom.strftime('%Y-%m-%dT00:00:00+03:00')
            
            daysTmp -= daysLoadPeriod
            if daysTmp < 0:
                daysTmp = -1
            
            dateTo = dt.timestamp(dt.now()) - (daysTmp) * 24 * 60 * 60
            dateTo = dt.fromtimestamp(dateTo)
            # Формат даты в Битриксе: 2017-01-21T18:12:15+03:00
            dateToB24 = dateTo.strftime('%Y-%m-%dT00:00:00+03:00')
       
            ID = 0
            LIMIT = 50
            
            while LIMIT > 0:
                payload = {
                    'ORDER[ID]': 'ASC',
                    'FILTER[>ID]': ID,
                    'FILTER[>LAST_UPDATED]': dateFromB24,
                    'FILTER[<LAST_UPDATED]': dateToB24,
                    'FILTER[!PROVIDER_ID]': 'TASKS',
                    'SELECT[]': {
                        'ID',
                        'OWNER_ID',
                        'AUTHOR_ID',
                        'TYPE_ID',
                        'PROVIDER_ID',
                        'PROVIDER_TYPE_ID',
                        'ASSOCIATED_ENTITY_ID',
                        'SUBJECT',
                        'COMPLETED',
                        'STATUS',
                        'DESCRIPTION',
                        'LOCATION',
                        'CREATED',
                        'LAST_UPDATED',
                        'START_TIME',
                        'END_TIME',
                        'DEADLINE',
                        'DIRECTION',
                        'PRIORITY',
                        'DIRECTION',
                        'OWNER_TYPE_ID'
                        },
                    'START': -1
                }
                
                if userID != 'EMPTY':
                    payload.update({'FILTER[AUTHOR_ID]': userID})

                # Делаем запрос и преобразовываем.
                response = requests.post(url_param, payload)
                
                if response.ok == True:
                    result = response.json()['result']
                else:
                    msg = 'Ошибка загрузки списка активностей: ' + str(response) + ' ' + str(response.text)
                    logger.error(msg)
                    send_mail('------------------', '--------------', 'ERROR. Problem with -----------ipt.', msg)
                    checkResult = check(checkResult)
                    return response

                # Переопределяем ID и LIMIT
                LIMIT = len(result)
                if LIMIT > 0:
                    ID = result[-1]['ID']
                if LIMIT < 50:
                    LIMIT = 0

                dataB24 += result
                
                if i == 10:
                    logger.info('   Уже загружено ' + str(len(dataB24)) + ' активностей. С момента начала выгрузки прошло ' + str(round((dt.now()-start).total_seconds()/60,1)) + ' мин.')
                    i = 0
            
                i += 1
                
                time.sleep(wait)
    
    checkResult = check(checkResult)
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])

    #Создаем DataFrame из dict (словаря данных или массива данных)
    keysDict = dataDict[0].keys()
    df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)
    
    
    # Преобразовываем данные
    df.loc[:,'CREATED'] = df['CREATED'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
    df.loc[:,'START_TIME'] = df['START_TIME'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
    df.loc[:,'END_TIME'] = df['END_TIME'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
    df.loc[:,'DEADLINE'] = df['DEADLINE'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
    df.loc[:,'LAST_UPDATED'] = df['LAST_UPDATED'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
    
    def makeLinkToCRM(data):
        if data[0] == '1':
            return '=HYPERLINK("https://----------------/crm/lead/details/{0}/","Lead {0}")'.format(data[1])
        if data[0] == '2':
            return '=HYPERLINK("https://----------------/crm/deal/details/{0}/","Deal {0}")'.format(data[1])
        if data[0] == '3':
            return '=HYPERLINK("https://----------------/crm/contact/details/{0}/","Contact {0}")'.format(data[1])
        if data[0] == '4':
            return '=HYPERLINK("https://----------------/crm/company/details/{0}/","Company {0}")'.format(data[1])
        if data[0] == '5':
            return '=HYPERLINK("https://----------------/crm/invoice/show/{0}/","Invoice {0}")'.format(data[1])
        else: 
            return 'owner_type_id: ' + data[0] + ' owner_id: ' + data[1]

    activities = df[['ID']]
    activities['Direction'] = df['DIRECTION'].apply(lambda x: 'Incoming' if x == '1' else 'Outgoing')
    activities['Type'] = df['PROVIDER_TYPE_ID']
    activities['Completed'] = df['COMPLETED'].apply(lambda x: 'YES' if x == 'Y' else 'NO')
    activities['Priority'] = df['PRIORITY'].apply(lambda x: 'Важное' if x == '3' else '')
    activities['Created'] = df['CREATED']
    activities['Deadline'] = df[['END_TIME','DEADLINE']].apply(lambda x: max(x[0],x[1]), axis=1)
    activities['Subject'] = df['SUBJECT']
    activities['Body'] = df['DESCRIPTION']
    activities['Location'] = df['LOCATION']
    activities['LinkToCRM'] = df[['OWNER_TYPE_ID','OWNER_ID']].apply(makeLinkToCRM, axis=1)
    activities['ResponsibleID'] = df['AUTHOR_ID']
    activities['Owner_type'] = df['OWNER_TYPE_ID']
    activities['Owner_id'] = df['OWNER_ID']
    if changedDate == 'YES':
        activities['ChangedDate'] = df['LAST_UPDATED']
    
    # Убираем тело письма, т.к. они очень большие и с лишними символами.
    activities.loc[activities['Type']=='EMAIL','Body'] = '<Тело письма убрано>'
    
    # Убираем ID активностей, т.к. с помощью ID нельзя сделать переход именно на активность.
    activities.loc[:,'ID'] = ''
    
    logger.info('   Закончили загрузку активностей и преобразования. Загружено активностей: ' + str(len(activities)))
    
    return activities
    
# Функция загрузки задач по дате
def getTasks(days,USERS=[],changedDate='NO'):
    # USERS - это DataFrame с данными сотрудников.
    # Если в нём имеются сотрудники, то загружаем задачи только по ним.
    if len(USERS) > 0:
        users = USERS['ID'].unique()
    else:
        users = ['EMPTY']

    logger.info('Загружаем задачи.')
    start = dt.now()

    # method
    method_name = "tasks.task.list"
    
    # Адрес api метода для запроса get 
    url_param = url + method_name
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []
    i = 0

    checkResult = check()
    for userID in users:
        daysTmp = days
    
        while daysTmp > 0:
            dateFrom = dt.timestamp(dt.now()) - (daysTmp) * 24 * 60 * 60
            dateFrom = dt.fromtimestamp(dateFrom)
            # Формат даты в Битриксе: 2017-01-21T18:12:15+03:00
            dateFromB24 = dateFrom.strftime('%Y-%m-%dT00:00:00+03:00')
            
            daysTmp -= daysLoadPeriod
            if daysTmp < 0:
                daysTmp = -1
            
            dateTo = dt.timestamp(dt.now()) - (daysTmp) * 24 * 60 * 60
            dateTo = dt.fromtimestamp(dateTo)
            # Формат даты в Битриксе: 2017-01-21T18:12:15+03:00
            dateToB24 = dateTo.strftime('%Y-%m-%dT00:00:00+03:00')
    
            ID = 0
            LIMIT = 50
         
            while LIMIT > 0:
                payload = {
                    'order[ID]': 'ASC',
                    'filter[>ID]': {ID},
                    'filter[>CHANGED_DATE]': dateFromB24,
                    'filter[<CHANGED_DATE]': dateToB24,
                    'select[]': {
                        'ID',
                        'CREATED_DATE',
                        'DATE_START',
                        'CLOSED_DATE',
                        'END_DATE_PLAN',
                        'DEADLINE',
                        'TITLE',
                        'DESCRIPTION',
                        'PRIORITY',
                        'STATUS',
                        'CREATED_BY',
                        'CLOSED_BY',
                        'RESPONSIBLE_ID',
                        'UF_CRM_TASK',
                        'CHANGED_DATE'
                    },
                    'start': -1
                }
                
                if userID != 'EMPTY':
                    payload.update({'filter[RESPONSIBLE_ID]': userID})

                # Делаем запрос и преобразовываем.
                response = requests.post(url_param, payload)
                
                if response.ok == True:
                    result = response.json()['result']['tasks']
                else:
                    msg = 'Ошибка загрузки списка задач: ' + str(response) + ' ' + str(response.text)
                    logger.error(msg)
                    send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
                    checkResult = check(checkResult)
                    return response
                
                # Переопределяем ID и LIMIT
                LIMIT = len(result)
                if LIMIT > 0:
                    ID = result[-1]['id']
                if LIMIT < 50:
                    LIMIT = 0

                dataB24 += result
                
                if i == 10:
                    print('   Уже загружено ' + str(len(dataB24)) + ' задач. С момента начала выгрузки прошло ' + str(round((dt.now()-start).total_seconds()/60,1)) + ' мин.')
                    i = 0
            
                i += 1
                
                time.sleep(wait)

    checkResult = check(checkResult)
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])
    
    if len(dataDict)>0:
        #Создаем DataFrame из dict (словаря данных или массива данных)
        keysDict = dataDict[0].keys()
        df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)
        
        # Преобразовываем данные
        df.loc[:,'closedDate'] = df['closedDate'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00') if x else x)
        df.loc[:,'dateStart'] = df['dateStart'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00') if x else x)
        df.loc[:,'createdDate'] = df['createdDate'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00'))
        df.loc[:,'deadline'] = df['deadline'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00') if x else x)
        df.loc[:,'endDatePlan'] = df['endDatePlan'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00') if x else x)
        df.loc[:,'changedDate'] = df['changedDate'].apply(lambda x: dt.strptime(x,'%Y-%m-%dT%H:%M:%S+03:00') if x else x)
        
        def makeLinkToTask(data):
            tmp = data[0]['id']
            return '=HYPERLINK("https://-------------------------/company/personal/user/{0}/tasks/task/view/{1}/", {1})'.format(tmp,data[1])

        def makeLinkToCRM(data):
            if data:
                entity, entity_id = data[0].split('_')
                if entity == 'L':
                    return '=HYPERLINK("https://-------------------------/crm/lead/details/{0}/","Lead {0}")'.format(entity_id)
                if entity == 'D':
                    return '=HYPERLINK("https://-------------------------/crm/deal/details/{0}/","Deal {0}")'.format(entity_id)
                if entity == 'C':
                    return '=HYPERLINK("https://-------------------------/crm/contact/details/{0}/","Contact {0}")'.format(entity_id)
                if entity == 'CO':
                    return '=HYPERLINK("https://-------------------------/crm/company/details/{0}/","Company {0}")'.format(entity_id)
                else: 
                    return 'owner_type_id: ' + entity + ' owner_id: ' + entity_id
            else:
                return
                
        def searchEntityType(x):
            if x:
                for i in x:
                    t,ID = i.split('_')
                    if t == 'C':
                        return '3'
                    elif t == 'CO':
                        return '4'
                    elif t == 'D':
                        return '2'
            else:
                return
        
        def searchEntityID(x):
            if x:
                for i in x:
                    t,ID = i.split('_')
                    if t == 'C':
                        return ID
                    elif t == 'CO':
                        return ID
                    elif t == 'D':
                        return ID
            else:
                return

        df['ID'] = df[['responsible','id']].apply(makeLinkToTask, axis=1)
        tasks = df[['ID']]
        tasks['Direction'] = ''
        tasks['Type'] = 'TASK'
        tasks['Completed'] = df['status'].apply(lambda x: 'YES' if x == '5' else 'NO')
        tasks['Priority'] = df['priority'].apply(lambda x: 'Важное' if x == '2' else '')
        tasks['Created'] = df['createdDate']
        tasks['Deadline'] = df[['deadline','endDatePlan']].apply(lambda x: max(x[0],x[1]) if x[0] and x[1] else x[0], axis=1)
        tasks['Subject'] = df['title']
        tasks['Body'] = df['description']
        tasks['Location'] = ''
        tasks['LinkToCRM'] = df['ufCrmTask'].apply(makeLinkToCRM)
        tasks['ResponsibleID'] = df['responsible'].apply(lambda x: x['id'])
        tasks['Owner_type'] = df['ufCrmTask'].apply(searchEntityType)
        tasks['Owner_id'] = df['ufCrmTask'].apply(searchEntityID)
        if changedDate == 'YES':
            tasks['ChangedDate'] = df['changedDate']
        
        logger.info('   Закончили загрузку задач. Загружено: ' + str(len(tasks)))
    
        return tasks
    else:
        logger.info('   0 выгруженных задач.')
        return []
    
# Функция загрузки списка департаментов и их руководителей.
def getDepartments():

    logger.info('Загружаем список подразделений.')

    method_name = "department.get"
    url_param = url + method_name
    
    checkResult = check()
    response = requests.post(url_param)
    if response.ok == True:
        result = response.json()['result']
    else:
        msg  = 'Ошибка загрузки списка подразделений: ' + str(response) + ' ' + str(response.text)
        logger.error(msg)
        send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
        checkResult = check(checkResult)
        return response
        
    dataDict = {}
    for j in range(0,len(result)):
        dataDict[j] = dict(result[j])

    #Создаем DataFrame из dict (словаря данных или массива данных)
    keysDict = dataDict[1].keys()
    departments = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)
    
    # Список ID подразделений у которых нет руководителя.
    ids = departments[(departments['UF_HEAD'].isnull()) | (departments['UF_HEAD']=='0')]['ID'].unique()
    
    # По каждому такому подразделению узнаём родительское подразделение
    for i in ids:

        ID = i
        method_name = "department.get"
        url_param = url + method_name

        # Пока checkSearch = 1 мы осуществляем поиск родительского подразделения, у которого имеется руководитель.
        checkSearch = 1
        while checkSearch == 1:
            payload = {'ID': ID}
            
            response = requests.post(url_param,payload)
            
            if response.ok == True:
                result = response.json()['result']
            else:
                msg = 'Ошибка загрузки списка описания подразделения: ' + str(response) + ' ' + str(response.text)
                logger.error(msg)
                send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
                checkResult = check(checkResult)
                return response
            
            # Родительское поразделение.
            parent = result[0]['PARENT']

            # Если в изначальном списке имеется руководитель у вышестоящего подразделения,
            # то записываем его.
            if len(departments[departments['ID']==parent]['UF_HEAD']) > 0:
                uf_head = departments[departments['ID']==result[0]['PARENT']]['UF_HEAD'].iloc[0]
                departments.loc[departments['ID']==i,'UF_HEAD'] = result[0]['UF_HEAD'] = uf_head
                checkSearch = 0
            else:
                ID = parent

                #Если мы дошли до головного подразделения и в нём нет руководителя, то прекращаем поиск.
                if parent == '1':
                    checkSearch = 0
    
    checkResult = check(checkResult)
    #logger.info('Загрузили подразделения и составили список руководителей.')
    del departments['SORT']
    
    logger.info('   Закончили загрузку списка подразделений. Загружено: ' + str(len(departments)))
    
    return departments
    
# Получаем список сотрудников
def getUsers(usersUnique):
    
    # usersUnique - это те пользователи, по которым необходимы данные.
    
    logger.info('Загружаем список сотрудников.')
    
    method_name = "user.get"
    url_param = url + method_name
    
    payload = { 
            'ORDER[ID]': 'ASC'
        }
    
    checkResult = check()
    response = requests.post(url_param,payload)
    
    if response.ok == True:
        result = response.json()['result']
    else:
        msg = 'Ошибка загрузки списка сотрудников: ' + str(response) + ' ' + str(response.text)
        logger.error(msg)
        send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
        checkResult = check(checkResult)
        return response
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []

    """
    Не работает FILTER ни в каком виде. Пробовал писать в разных форматах и пробовал с user.search.
    В общем никак, поэтому получаем список первых пользователей, а которых не хватает - добираем.
    Причём их можно добирать только по одному, т.е. передать список ID также не получается.
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []
    
    # Изначально ставим id = 0, каждый раз будем изменять ID при запросах, при условии, что start = -1
    # Это ускоряет запросы по API в 50 раз. Источник тут: https://dev.1c-bitrix.ru/rest_help/rest_sum/start.php
    # Останавливаем запросы, когда limit будет равен 0.
    ID = 0
    LIMIT = 50
    
    while LIMIT > 0:
        payload = { 
            'ORDER[ID]': 'ASC',
            'FILTER[>ID]': ID
        }

        # Делаем запрос и преобразовываем.
        response = requests.post(url_param, payload)
        if response.ok == True:
            result = response.json()['result']
        else:
            #logger.info('Ошибка загрузки списка сотрудников.')
            return 'ERROR'
        
        # Переопределяем ID и LIMIT
        LIMIT = len(result)
        if LIMIT > 0:
            ID = int(result[-1]['ID'])
        if LIMIT < 50:
            LIMIT = 0

        dataB24 += result
        
        time.sleep(wait)
    """
    dataB24 += result
    
    # Теперь загрузим 50 с конца
    payload = { 
            'ORDER[ID]': 'DESC'
        }
    
    response = requests.post(url_param,payload)
    
    if response.ok == True:
        result = response.json()['result']
    else:
        msg = 'Ошибка загрузки списка сотрудников: ' + str(response) + ' ' + str(response.text)
        logger.error(msg)
        send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
        checkResult = check(checkResult)
        return response
    
    dataB24 += result
    
    
    # Из списка уникальных сотрудников который нам необходим уберём тех, кого только что загрузили.
    for user in dataB24:
        if usersUnique.count(user['ID']) > 0:
            usersUnique.remove(user['ID'])
        else:
            continue
      
    logger.info('   Уже загрузили контактов: ' + str(len(dataB24)))
    logger.info('   Загружаем данные по каждому сотруднику отдельно. Всего к загрузке: ' + str(len(usersUnique)) + ' ед.')
    
    # Загружаем по ним данные.
    for ID in usersUnique:
        payload = {
            'FILTER[ID]': ID
        }
        
        response = requests.post(url_param,payload)
        if response.ok == True:
            result = response.json()['result']
        else:
            msg = 'Ошибка загрузки данных по сотруднику с ID ' + ID + ': ' + str(response) + ' ' + str(response.text)
            logger.error(msg)
            send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
            checkResult = check(checkResult)
            return response
        
        dataB24 += result
        
        time.sleep(wait)
    
    checkResult = check(checkResult)
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])

    #Создаем DataFrame из dict (словаря данных или массива данных)
    keysDict = dataDict[0].keys()
    df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)

    df['FULL_NAME'] = df['LAST_NAME'].fillna('') + ' ' + df['NAME'].fillna('') + ' ' + df['SECOND_NAME'].fillna('')
    users = df[['ID','ACTIVE','FULL_NAME','EMAIL','PERSONAL_MOBILE','PERSONAL_PHONE','UF_DEPARTMENT']]
    users.loc[:,'UF_DEPARTMENT'] = users['UF_DEPARTMENT'].apply(lambda x: str(x[0]))
    
    logger.info('   Выгрузили список сотрудников. Выгружено: ' + str(len(users)))
    users.drop_duplicates(subset='ID',inplace=True)
    logger.info('   Кол-во строк в users после удаления дубликатов: ' + str(len(users)))
    
    return users

# Получаем список сотрудников
def getUsersByDepartments(deps):
    
    # deps - это список департаментов, откуда берём пользователей
    
    logger.info('Загружаем список сотрудников по списку департаментов.')
    
    method_name = "user.get"
    url_param = url + method_name
    
    checkResult = check()
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []
    
    for dep in deps:
            
        payload = { 
                'ORDER[ID]': 'ASC',
                'UF_DEPARTMENT': dep
            }
        
        response = requests.post(url_param,payload)
    
        if response.ok == True:
            result = response.json()['result']
        else:
            msg = 'Ошибка загрузки списка сотрудников по департаментам ' + str(response) + ' ' + str(response.text)
            logger.error(msg)
            send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
            checkResult = check(checkResult)
            return response
    
        dataB24 += result
        
        logger.info('   Из департамента ID' + dep + ' загрузили ' + str(len(dataB24)) + ' чел.')
   
    checkResult = check(checkResult)
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])

    #Создаем DataFrame из dict (словаря данных или массива данных)
    keysDict = dataDict[0].keys()
    df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)

    df['FULL_NAME'] = df['LAST_NAME'].fillna('') + ' ' + df['NAME'].fillna('') + ' ' + df['SECOND_NAME'].fillna('')
    users = df[['ID','ACTIVE','FULL_NAME','EMAIL']]
    
    logger.info('   Выгрузили список сотрудников. Выгружено: ' + str(len(users)))
    users.drop_duplicates(subset='ID',inplace=True)
    logger.info('   Кол-во строк в users после удаления дубликатов: ' + str(len(users)))
    
    return users
    
# Загрузка данных сущностей по их ID. 
def getEntityDataByIDS(ids,method_name):
    ids.sort()
    
    logger.info('Загружаем данные по сущностям, используя их ID. Метод: ' + method_name + '.')
    
    start = dt.now()
    
    # Адрес api метода для запроса get 
    url_param = url + method_name
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []
    
    # Берём минимальный ID из нужных контактов и начинаем поиск с него
    if len(ids) > 0:
        ID = min(ids)
        LIMIT = 50
        i = 0
        checkResult = check()
    else:
        logger.info('   Выгружено 0 сущностей.')
        return 0

    while LIMIT > 0:
        payload = {
            'ORDER[ID]': 'ASC',
            'FILTER[>=ID]': ID,
            'START': -1
        }
        
        if method_name == 'crm.contact.list':
            payload.update({
            'SELECT[]': {
                'ID',
                'COMPANY_ID',
                'LAST_NAME',
                'NAME',
                'SECOND_NAME'
            }})
   
        if method_name == 'crm.company.list':
            payload.update({
            'SELECT[]': {
                'ID',
                'TITLE',
                'UF_CRM_1605452166'  # Период контакта (целое число)
            }})
            
        if method_name == 'crm.deal.list':
            payload.update({
            'SELECT[]': {
                'ID',
                'CONTACT_ID',
                'COMPANY_ID'
            }})

        # Делаем запрос и преобразовываем.
        response = requests.post(url_param, payload)
        
        if response.ok == True:
            result = response.json()['result']
        else:
            msg = 'Ошибка загрузки данных по сущностям по методу ' + method_name + ': ' + str(response) + ' ' + str(response.text)
            logger.error(msg)
            send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
            checkResult = check(checkResult)
            return response
        
        # Переопределяем ID и LIMIT
        LIMIT = len(result)
        if LIMIT > 0:
            ID = result[-1]['ID']
            
            # Найдём ближайший контакт следующий за последним загруженным.
            # Если загруженный последний ID менее максимального ID из списка.
            if int(ID) < int(max(ids)):
                for k in ids:
                    if int(k) > int(ID):
                        ID = k
                        break
            else:
                LIMIT = 0
            
            
        if LIMIT < 50:
            LIMIT = 0

        dataB24 += result
        
        if i == 10:
            logger.info('   Уже загружено ' + str(len(dataB24)) + ' сущностей. С момента начала выгрузки прошло ' + str(round((dt.now()-start).total_seconds()/60,1)) + ' мин.')
            i = 0
        
        i += 1
        time.sleep(wait)
    
    checkResult = check(checkResult)
    
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])
    
    if len(dataDict)>0:
        #Создаем DataFrame из dict (словаря данных или массива данных)
        keysDict = dataDict[0].keys()
        df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)
        
        logger.info('   Выгрузили список сущностей. Строк перед удалением дубликатов: ' + str(len(df)))
        df.drop_duplicates(subset='ID',inplace=True)
        logger.info('   Кол-во строк после удаления дубликатов: ' + str(len(df)))

        return df
    else:
        logger.info('   Выгружено 0 сущностей.')
        return 0        

        
# Функция загрузки списка всех компаний
def getCompanies():
    
    start = dt.now()
    logger.info('Приступаем к загрузке всех компаний в CRM.')
    
    # method
    method_name = "crm.company.list"
    
    # Адрес api метода для запроса get 
    url_param = url + method_name
    
    # Массив для хранения загруженных из Б24 данных
    dataB24 = []

    # Изначально ставим id = 0, каждый раз будем изменять ID при запросах, при условии, что start = -1
    # Это ускоряет запросы по API в 50 раз. Источник тут: https://dev.1c-bitrix.ru/rest_help/rest_sum/start.php
    # Останавливаем запросы, когда limit будет равен 0.
    ID = 0
    LIMIT = 50
    checkResult = check()
    
    i = 0
    while LIMIT > 0:
        payload = {
            'ORDER[ID]': 'ASC',
            'FILTER[>ID]': ID,
            'SELECT[]': {
                'ID',
                'TITLE',
                'UF_CRM_1605452166',                # Период контакта (целое число)
                'ASSIGNED_BY_ID'
                },
            'START': -1
        }

        # Делаем запрос и преобразовываем.
        response = requests.post(url_param, payload)
        
        if response.ok == True:
            result = response.json()['result']
        else:
            msg = 'Ошибка загрузки списка всех компаний ' + str(response) + ' ' + str(response.text)
            logger.error(msg)
            send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
            checkResult = check(checkResult)
            return response
        
        # Переопределяем ID и LIMIT
        LIMIT = len(result)
        if LIMIT > 0:
            ID = result[-1]['ID']
        if LIMIT < 50:
            LIMIT = 0

        dataB24 += result
        
        if i == 10:
            logger.info('   Уже загружено ' + str(len(dataB24)) + ' компаний. С момента начала выгрузки прошло ' + str(round((dt.now()-start).total_seconds()/60,1)) + ' мин.')
            i = 0
        
        i += 1
        time.sleep(wait)
    
    checkResult = check(checkResult)
    
    dataDict = {}
    for j in range(0,len(dataB24)):
        dataDict[j] = dict(dataB24[j])
    
    if len(dataDict)>0:
        #Создаем DataFrame из dict (словаря данных или массива данных)
        keysDict = dataDict[0].keys()
        df = pd.DataFrame.from_dict(dataDict, orient='index',columns=keysDict)
        
        def makeLink(data):
            if data[1]:
                tmp = str(data[1]).replace('"','')
                return '=HYPERLINK("https://-------------------------/crm/company/details/{0}/","{1}")'.format(data[0],tmp)
            else:
                return
        df['TITLE'] = df[['ID','TITLE']].apply(makeLink, axis=1)
        
        # Иногда почему-то столбцы переставляются местами, поставим в нужном нам порядке:
        newdf = df[['ID','TITLE','UF_CRM_1605452166','ASSIGNED_BY_ID']]
        newdf.rename(columns={'UF_CRM_1605452166': 'PeriodOfContact'}, inplace=True)
        
        logger.info('   Выгрузили компаний: ' + str(len(newdf)))
        
        return newdf
    else:
        return 0
        