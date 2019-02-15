#!/usr/bin/env python
# -*- coding=utf-8 -*-
# @version: v1.0
# @author: jiang.ml
# @contact: jiang.ml.sz@belle.com.cn
# @file: chack_return_date.py
# @time: 2019/1/22 16:02

import sys
reload(sys)
sys.setdefaultencoding('utf8')
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import time
import pandas as pd
import zipfile
import xlwt
from impala.dbapi import connect
conn = connect(host='10.240.20.29', port=21050)

import cx_Oracle as oracle
orc_conn = oracle.connect('bi_report/YG&*(123{yougou}@10.240.20.141:1521/report_141')

def Hive_sql(cdh_sql):
    cdh_data = pd.read_sql(cdh_sql,conn)
    return cdh_data

def Orc_sql(orc_sql):
    orc_date = pd.read_sql(orc_sql,orc_conn)
    return orc_date

def SendMail(table_name,max_time):
    today = time.strftime('%Y-%m-%d_%H', time.localtime(time.time()))
    server = {'name': 'smtp.exmail.qq.com', 'user': 'daily_report@belle.com.cn', 'passwd': 'abcd@12H'}
    fro = 'jiang.ml<daily_report@belle.com.cn>'
    to = ['jiang.ml.sz@belle.com.cn']#,'liang.x.sz@belle.com.cn','liu.qm.sz@belle.com.cn','guan.xl.sz@belle.com.cn'
    from email.header import Header
    msg = MIMEMultipart()
    msg['From'] = fro
    msg['Subject'] = Header('退货数据警告{0}'.format(today), 'utf-8')
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    content = 'Dear All：' \
              '     \n退货数据有问题{0},表{1}更新失败，望知悉.\n更新截止时间到{2}'.format(today,table_name,max_time)
    cont = MIMEText(content, 'plain', 'utf-8')
    msg.attach(cont)

    import smtplib
    smtp = smtplib.SMTP(server['name'])
    smtp.login(server['user'], server['passwd'])
    smtp.sendmail(fro, to, msg.as_string())
    smtp.close()

if __name__ == '__main__':
    if (len(sys.argv) == 3):
        today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

        se_word = sys.argv[1]

        table_name = sys.argv[2]

        monitor_sql = "select substr(max(%),1,10) from %"%(se_word,table_name)
        max_time = Hive_sql(monitor_sql)

        if(max_time == str(today)):
            print("-----------退货率数据依赖表更新成功-----------------")
        else:
            SendMail(table_name,max_time)
    else:
        print("参数错误,参数应该是:字段名,表名")

