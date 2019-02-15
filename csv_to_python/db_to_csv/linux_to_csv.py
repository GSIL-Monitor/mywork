# -*- coding:utf-8 -*-
#!/usr/bin/env python

from datetime import datetime
import datetime as dy
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import time
import pandas as pd
import xlwt
from impala.dbapi import connect
conn = connect(host='10.240.20.29', port=21050)

def Hive_data(df_sql):

    df_now = pd.read_sql(df_sql,conn)

    # df_now.columns = [u'品牌',u'商品编码（款色编码）',u'尺码',u'条形码',u'商品分类',u'缺货风险类型',u'总销量（支付不含取消）',u'派至电商仓',u'派至线下',\
    #          u'待派数量',u'电商库存',u'O2O线共享库存',u'总库存（电商+O2O）',u'线下可用库存',u'全国',u'C-东北',u'D-华北',\
    #           u'E-鲁豫',u'F-西北',u'G-西南',u'H-华中',u'I-华东',u'K-华南']

    return df_now
def write_csv(data_now):

    data_now.to_csv('/home/jiang.ml/data/O2O退货汇总数据(07.01-10.31).csv',index=False,sep=',',encoding='utf-8-sig')


if __name__ == '__main__':
    df_sql = "select * from bi_report.all_return_collect_detail where report_date >='2018-07-01' and report_date <='2018-10-31' and send_type = '线下';"
    # 1.载入数据
    print("---------- 1.load data ------------")
    data_now = Hive_data(df_sql)
    # 2.写入附件
    print("---------- 2.writer csv---------")
    write_csv(data_now)


