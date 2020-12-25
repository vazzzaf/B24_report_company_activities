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
days = 180

# ID отделов, по которым отправляем
# 14 - Business Development Department RU
# 190 - Business Development Department UA
configDepartments = ['14','190']

# Список адресов кому отправляем отчёт по умолчанию.
# Дополнительно будут отправлены отчёты по пользователям из configDepartments за вычетом send_to_exclude.
send_to=['------------------','------------------','-----------------------']
send_to_exclude=['266','188'] # Исключить CRM Bot и Olga --------

#################################

# Настраиваем логирование.
logger = logging.getLogger()
logger.level = logging.INFO
logger.addHandler(logging.StreamHandler(sys.stderr))
logging.getLogger("requests").setLevel(logging.ERROR)

START = dt.now()
logger.info('Начинаем работу: ' + str(START) + ' Загружаем компании без активностей за последние ' + str(days) + ' дн.')

# Загружаем данные из Б24.
users = getUsersByDepartments(configDepartments)# список пользователей.
if hasattr(users,'ok'):
    sys.exit(users)
    
# Составляем список сотрудников кому отправлять отчёт.
for address in users[(~users['ID'].isin(send_to_exclude))&(users['ACTIVE']==True)]['EMAIL'].unique():
    if len(address) > 2:
        send_to.append(address)

if len(send_to) > 1:
    logger.info('Отправим письмо по адресам: ' + str(send_to))
else:
    msg = 'Не смогли выяснить кому отправлять письма - останавливаем.'
    send_mail('-------------------------', '-------------------------', 'ERROR. Problem with ------------------------- script.', msg)
    sys.exit(msg)

tasks = getTasks(days,users,'YES') # Задачи
if hasattr(tasks,'ok'):
    sys.exit(tasks)

activities = getActivities(days,users,'YES') # Активности
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
    
df = data
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

# Загружаем список всех компаний.
companiesAll = getCompanies()
if hasattr(companiesAll,'ok'):
    sys.exit(companiesAll)
    
# Загружаем список всех нужных сотрудников по компаниям.
tmpLst = []
for tmp in companiesAll['ASSIGNED_BY_ID'].unique():
    if int(tmp) in tmpLst:
        continue
    else:
        tmpLst.append(int(tmp))
usersAll = getUsers(tmpLst)

# Создадим датайфреймы, оставив только ID и название.
dfCompanies = companiesAll[['ID','TITLE']]
dfCompanies.rename(columns={'ID':'COMPANY_ID','TITLE':'COMPANY_TITLE'}, inplace=True)

# Создаём датайфрейм с типом сущности и её ID, а также с CONTACT_ID и COMPANY_ID.
dfIDS = companiesAll[['ID']]
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
df2 = df[['Type','ChangedDate','Owner_id','Owner_type']].merge(dfIDS, how='left', on=['Owner_type','Owner_id'])

# Убираем активности без компаний и удаляем лишние столбцы.
df2 = df2[~df2['COMPANY_ID'].isnull()].reset_index(drop=True)
df2.drop(columns={'Owner_id','Owner_type','CONTACT_ID'}, inplace=True)

# Создадим новый датафрейм с уникальным ID компании и последней датой изменения.
dfChangedDate = df2.groupby(['COMPANY_ID'])['ChangedDate'].max().to_frame(name = 'ChangedDate').reset_index()

# Объединим с информацией по дате последней активности и с ответственными по компании.
dfResult = companiesAll.merge(dfChangedDate, how='left', left_on='ID', right_on='COMPANY_ID')

# Добавим столбец с информацией когда была последняя активность.
dfResult.loc[:,'DaysElapsed'] = dfResult['ChangedDate'].apply(lambda x: str((dt.now()-x).days) if str(x) != 'NaT' else str('> ' + str(days) + 'дн.'))

# Удалим лишние столбцы.
dfResult.drop(columns=['COMPANY_ID'], inplace=True)

# Добавим информацию по сотрудникам.
usersAll.rename(columns={'ID':'ASSIGNED_BY_ID','FULL_NAME':'ASSIGNED_BY'}, inplace=True)
dfResult = dfResult.merge(usersAll[['ASSIGNED_BY_ID','ASSIGNED_BY']], how='left', on='ASSIGNED_BY_ID')
dfResult.drop(columns=['ASSIGNED_BY_ID'], inplace=True)

# Изменим колонку на тип "время"
df.loc[:,'Deadline'] = pd.to_datetime(df['Deadline'])

today = dt.now().strftime('%Y-%m-%d')
filename = 'CompaniesWithoutActivities_' + today + '.xlsx'

logger.info('Собираем Excel файлы и рассылаем.')

# Функция по форматированию листов
def formatSheet(worksheet):
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
    
    num_format = workbook.add_format({
        'text_wrap': True,
        'align': 'center',
        'valign': 'vcenter',
        'num_format': '0'
    })
    
    worksheet.set_column("A:A", 7, text_format)
    worksheet.set_column("B:B", 65, link_format)
    worksheet.set_column("C:C", 16, text_format)
    worksheet.set_column("D:D", 16, num_format)
    worksheet.set_column("E:E", 32, text_format)

# Specify a writer
writer = pd.ExcelWriter(filename, engine='xlsxwriter')

# Все компании
fileData = dfResult.drop(columns=['ChangedDate'])

if len(fileData) > 0:
    # Write your DataFrame to a file     
    fileData.to_excel(writer, 'All_Companies', index=False)
    
    workbook  = writer.book
    worksheet = writer.sheets['All_Companies']
    formatSheet(worksheet)
    
# Не было активности более 30 дней.
fileData = dfResult
fileData.loc[:,'Tmp'] = fileData['ChangedDate'].apply(lambda x: (dt.now()-x).days)
fileData = fileData[~(fileData['Tmp'] < 30)]
fileData.drop(columns=['ChangedDate','Tmp'], inplace=True)

if len(fileData) > 0:
    # Write your DataFrame to a file     
    fileData.to_excel(writer, 'NoActivitiyMoreThan30Days', index=False)
    
    workbook  = writer.book
    worksheet = writer.sheets['NoActivitiyMoreThan30Days']
    formatSheet(worksheet)

# Все компании по каждому сотруднику отдельно
dfResult.drop(columns=['ChangedDate','Tmp'], inplace=True)
for user in dfResult['ASSIGNED_BY'].unique():
    
    # Данные по сотруднику
    fileData = dfResult[dfResult['ASSIGNED_BY'] == user]
    
    if len(fileData) > 0:
        
        # Write your DataFrame to a sheet     
        fileData.to_excel(writer, str(user).replace('"',''), index=False)
        
        workbook  = writer.book
        worksheet = writer.sheets[str(user).replace('"','')]
        formatSheet(worksheet)
        
writer.save()

send_mail('-------------------------', send_to, 'Companies Without Activities ' + today, 'Файл в приложении.', [ './' + filename ])
os.remove('./' + filename)

logger.info('Закончили. Затрачено ' + str(round((dt.now()-START).total_seconds()/60,1)) + ' мин.')
