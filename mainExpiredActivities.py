import requests 
import pandas as pd
from datetime import datetime as dt
import xlsxwriter
import time
import datetime
import os
import sys
import logging

# Импорт функций.
from functions import *

pd.options.mode.chained_assignment = None  # default='warn'

#############CONFIG#############
# За сколько последних дней загружать.
days = 7

# ID отделов, по которым отправляем
# 14 - Business Development Department RU
# 190 - Business Development Department UA
configDepartments = ['14','190']

# Список адресов кому отправляем отчёт.
send_to=['-------------------','------------------','-----------------']

#################################

# Настраиваем логирование.
logger = logging.getLogger()
logger.level = logging.INFO
logger.addHandler(logging.StreamHandler(sys.stderr))
logging.getLogger("requests").setLevel(logging.ERROR)

START = dt.now()
logger.info('Начинаем работу: ' + str(START) + ' Загружаем компании с просроченными активностями за последние ' + str(days) + ' дн.')

# Загружаем данные из Б24.
users = getUsersByDepartments(configDepartments)# список пользователей.
if hasattr(users,'ok'):
    sys.exit(users)

tasks = getTasks(days,users) # Задачи
if hasattr(tasks,'ok'):
    sys.exit(tasks)

activities = getActivities(days,users) # Активности
if hasattr(activities,'ok'):
    sys.exit(activities)

if len(activities) > 0 and len(tasks) > 0:
    data = pd.concat([activities,tasks],ignore_index=True)
elif len(activities) > 0:
    data = activities
elif len(tasks) > 0:
    data = tasks
else:
    logger.info('Нет задач и активностей. Останавливаем.')
    sys.exit()

# Объединяем таблицу с задачами и активностями с таблицой пользователей.
users.rename(columns={'ID': 'ResponsibleID'}, inplace=True)
df = data.merge(users, how='left', on='ResponsibleID')
del df['ResponsibleID']

# Соберём список ID сделок и по ним узнаем привязанные контакты и компании.
tmpLst = []
for tmp in data[data['Owner_type']=='2']['Owner_id'].unique():
    if int(tmp) in tmpLst:
        continue
    else:
        tmpLst.append(int(tmp))
deals = getEntityDataByIDS(tmpLst,'crm.deal.list')

tmpLstStr = []
for i in tmpLst:
    tmpLstStr.append(str(i))
if hasattr(deals, 'loc'):
    deals = deals.loc[deals['ID'].isin(tmpLstStr)] # Удалили лишние загруженные строки.
    logger.info('Удалили лишние загруженные сделки, осталось: ' + str(len(deals)))

# Дополним контактами из сделок список уникальных ID контактов
# Затем по ним и по ID контактов из активностей и задач узнаем привязанные компании.
tmpLst = []
if hasattr(deals, 'loc'):
    for tmp in deals[(~deals['CONTACT_ID'].isnull())&(deals['CONTACT_ID'] != '0')]['CONTACT_ID'].unique():
        if int(tmp) in tmpLst:
            continue
        else:
            tmpLst.append(int(tmp))

for tmp in data[data['Owner_type']=='3']['Owner_id'].unique():
    if int(tmp) in tmpLst:
        continue
    else:
        tmpLst.append(int(tmp))
contacts = getEntityDataByIDS(tmpLst,'crm.contact.list')

tmpLstStr = []
for i in tmpLst:
    tmpLstStr.append(str(i))
if hasattr(contacts, 'loc'):
    contacts = contacts.loc[contacts['ID'].isin(tmpLstStr)] # Удалили лишние загруженные строки.
    logger.info('Удалили лишние загруженные контакты, осталось: ' + str(len(contacts)))

# Соберём список ID компаний.
tmpLst = []
for tmp in data[data['Owner_type']=='4']['Owner_id'].unique():
    if int(tmp) in tmpLst:
        continue
    else:
        tmpLst.append(int(tmp))

# Зная привязанные к контактам и сделкам ID компаний, дополним из них список уникальных ID компаний.
if hasattr(contacts, 'loc'):
    for tmp in contacts[(~contacts['COMPANY_ID'].isnull())&(contacts['COMPANY_ID'] != '0')]['COMPANY_ID'].unique():
        if int(tmp) in tmpLst:
            continue
        else:
            tmpLst.append(int(tmp))
if hasattr(deals, 'loc'):
    for tmp in deals[(~deals['COMPANY_ID'].isnull())&(deals['COMPANY_ID'] != '0')]['COMPANY_ID'].unique():
        if int(tmp) in tmpLst:
            continue
        else:
            tmpLst.append(int(tmp))
companies = getEntityDataByIDS(tmpLst,'crm.company.list')

tmpLstStr = []
for i in tmpLst:
    tmpLstStr.append(str(i))
companies = companies.loc[companies['ID'].isin(tmpLstStr)] # Удалили лишние загруженные строки.
logger.info('Удалили лишние загруженные компании, осталось: ' + str(len(companies)))

# Создадим датайфреймы, оставив только ID и название.
contacts['CONTACT_NAME'] = contacts['LAST_NAME'].fillna('') + ' ' + contacts['NAME'].fillna('') + ' ' + contacts['SECOND_NAME'].fillna('')
dfContacts = contacts[['ID','CONTACT_NAME']]
dfCompanies = companies[['ID','TITLE']]
dfContacts.rename(columns={'ID':'CONTACT_ID'}, inplace=True)
dfCompanies.rename(columns={'ID':'COMPANY_ID','TITLE':'COMPANY_TITLE'}, inplace=True)

# Создаём датайфрейм с типом сущности и её ID, а также с CONTACT_ID и COMPANY_ID.
dfIDS = companies[['ID']]
dfIDS['Owner_type'] = '4'
dfIDS['CONTACT_ID'] = None
dfIDS['COMPANY_ID'] = dfIDS['ID']
dfIDS.rename(columns={'ID':'Owner_id'}, inplace=True)

if hasattr(contacts, 'loc'):
    dfTmp = contacts[['ID']]
    dfTmp['Owner_type'] = '3'
    dfTmp['CONTACT_ID'] = dfTmp['ID']
    dfTmp['COMPANY_ID'] = contacts['COMPANY_ID']
    dfTmp.rename(columns={'ID':'Owner_id'}, inplace=True)

    dfIDS = pd.concat([dfIDS,dfTmp], axis=0, ignore_index=True)

if hasattr(deals, 'loc'):
    dfTmp = deals[['ID']]
    dfTmp['Owner_type'] = '2'
    dfTmp['CONTACT_ID'] = deals['CONTACT_ID']
    dfTmp['COMPANY_ID'] = deals['COMPANY_ID']
    dfTmp.rename(columns={'ID':'Owner_id'}, inplace=True)

    dfIDS = pd.concat([dfIDS,dfTmp], axis=0, ignore_index=True)

# Объёдиним данный датафрейм с итоговым.
df = df.merge(dfIDS, how='left', on=['Owner_type','Owner_id'])

# Дополняем итоговый массив именами контактов и компаний.
df = df.merge(dfContacts, how='left', on='CONTACT_ID')
df = df.merge(dfCompanies, how='left', on='COMPANY_ID')
df.fillna('', inplace=True)

# Добавим ссылки на сущности.
def makeLinkToContact(data):
    if data[1] != '':
        tmp = str(data[1]).replace('"','')
        return '=HYPERLINK("https://----------------------/crm/contact/details/{0}/","{1}")'.format(data[0],tmp)
    else:
        return
def makeLinkToCompany(data):
    if data[1] != '':
        tmp = str(data[1]).replace('"','')
        return '=HYPERLINK("https://----------------------/crm/company/details/{0}/","{1}")'.format(data[0],tmp)
    else:
        return
df.loc[:,'CONTACT_NAME'] = df[['CONTACT_ID','CONTACT_NAME']].apply(makeLinkToContact, axis=1)
df.loc[:,'COMPANY_TITLE'] = df[['COMPANY_ID','COMPANY_TITLE']].apply(makeLinkToCompany, axis=1)

# Удалим лишние колонки.
df.drop(labels={'Owner_type','Owner_id','CONTACT_ID','COMPANY_ID'}, axis=1, inplace=True)

# Изменим колонку на тип "время"
df.loc[:,'Deadline'] = pd.to_datetime(df['Deadline'])

# Отбираем просроченные и невыполненные задачи и активности.
dfExpired = df[(df['Completed']=='NO') & (df['Deadline']<dt.now())]
del dfExpired['Completed']

today = dt.now().strftime('%Y-%m-%d')
filename = 'expiredTasks_' + today + '.xlsx'

logger.info('Собираем Excel файлы и рассылаем.')
i = 0

if len(dfExpired) > 0:
    # Specify a writer
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    # Write your DataFrame to a file     
    dfExpired.to_excel(writer, 'Sheet1', index=False)
    
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    
    link_format = workbook.add_format({
        'color': 'blue',
        'underline': True,
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter'
    })

    text_format = workbook.add_format({
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter'
    })
    
    worksheet.set_column("A:A", 7, link_format)
    worksheet.set_column("B:D", None, text_format)
    worksheet.set_column("E:F", 18, text_format)
    worksheet.set_column("G:H", 28, text_format)
    worksheet.set_column("I:I", 14, text_format)
    worksheet.set_column("J:J", 14, link_format)
    worksheet.set_column("K:K", None, text_format)
    worksheet.set_column("L:L", 28, text_format)
    worksheet.set_column("M:N", 28, link_format)

    writer.save()
    
    send_mail('---------------------', send_to, 'ExpiredTasks ' + today, 'Файл в приложении.', [ './' + filename ])
    os.remove('./' + filename)
    i += 1
else:
    send_mail('---------------------', send_to, 'ExpiredTasks ' + today, 'Нет данных для выгрузки.')
    logger.info('Нечего выгружать.')

logger.info('Закончили. Затрачено ' + str(round((dt.now()-START).total_seconds()/60,1)) + ' мин.')
