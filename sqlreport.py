# coding=utf-8

"""
根据SQL生成报告并邮件发送
sqlreport.py
    -- sheets '商家,订单' # 导出的sheet名称列表,用逗号分割
    -- xls 'sss.xls' # 生成Excel文件的保存位置
    -- mailto  # 收件人列表
    -- mailsub '邮件标题'
    -- mailcontent '邮件正文'
"""

import re
import sys
import xlwt
import json
import MySQLdb
import smtplib
import traceback
import os.path
import importlib
from prettytable import PrettyTable

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
from io import BytesIO

"""
当程序中出现非ascii编码时，python的处理常常会报这样的错UnicodeDecodeError: 
‘ascii’ codec can’t decode byte 0x?? in position 1: ordinal not in range(128)，
python没办法处理非ascii编码，此时需要自己设置python的默认编码，一般设置为utf8的编码格式。
"""
importlib.reload(sys)


class DBConfig(object):
    # 数据库配置
    # __init__方法是不能有return值的
    def __init__(self, host, port, username, password, dbname):
        """
        @param host 数据库主机
        @param port 端口
        @param username 用户名
        @param password 密码
        @param dbname 数据库名称
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
    """
    打印一个实例化对象时，打印的其实是一个对象的地址。
    而通过__str__()函数就可以帮助我们打印对象中具体的属性值，或者你想得到的返回值。
    """
    def __str__(self):
        return "{host:%s, port:%d, username:%s, dbname:%s}" % (
            self.host, self.port, self.username, self.dbname
        )


class SendMailConfig(object):
    # 邮件发送配置
    def __init__(self, smtp_server, account, password, sender):
        """
        @param smtp_server SMTP服务器地址
        @param account 账号
        @param password 密码
        @param sender 发送者
        """
        self.smtp_server = smtp_server
        self.account = account
        self.password = password
        self.sender = sender

    def __str__(self):
        return "{smtp_server:%s, account:%s, password:%s, sender:%s}" % (
            self.smtp_server, self.account, self.password, self.sender
        )


class Table(object):
    # 表格
    def __init__(self, headers, rows):
        """
        @param headers 表头
        @param rows 数据行
        """
        self.headers = headers
        self.rows = rows

    def show(self):
        tbl = PrettyTable(self.headers)
        for row in self.rows:
            tbl.add_row(row)
        print(tbl)


# sql分隔符
SQL_SEPERATOR = ";"


def load_db_config(cfg_file_path):
    with open(cfg_file_path, "r") as f:
        config_json = json.loads(f.read())
        return DBConfig(**config_json)


def gen_connection(db_config):
    conn = MySQLdb.connect(
        host=db_config.host,
        user=db_config.username,
        passwd=db_config.password,
        db=db_config.dbname,
        charset="utf8"
    )
    return conn


def execute_sqllist(db_config, report_sql):
    conn = gen_connection(db_config)
    # 根据分号拆分sql语句
    report_sql_list = report_sql.split(SQL_SEPERATOR)
    print(report_sql_list)
    # 检查每个SQL格式是否正确，如果语句错误不予执行
    for report_sql in report_sql_list:
        if not QUERY_SQL_PATTERN.search(report_sql):
            report_sql_list.remove(report_sql)
    print(report_sql_list)
    cursor = conn.cursor()
    tables = []
    for sql in report_sql_list:
        tables.append(execute_sql(cursor, sql))
    conn.commit()
    cursor.close()
    conn.close()
    return tables


def execute_sql(db_cursor, sql):
    db_cursor.execute(sql)
    table = Table(
        headers=get_table_headers(db_cursor),
        rows=db_cursor.fetchall()
    )
    table.show()
    return table


def get_table_headers(db_cursor):
    return [i[0] for i in db_cursor.description]


QUERY_SQL_PATTERN = re.compile(r'.*?select.*?from\s+([^\s]*?)', re.I)


def gen_workbook(tables, sheet_names):
    # 创建一个工作簿对象
    workbook = xlwt.Workbook(encoding='utf-8')
    for idx, table in enumerate(tables):
        sheet_name = sheet_names[idx]
        # 创建一个工作表对象
        sheet = workbook.add_sheet(sheet_name)
        # 写入表头
        for header_idx, header_name in enumerate(table.headers):
            sheet.write(0, header_idx, header_name)
        # 写入行
        for row_idx, row in enumerate(table.rows):
            for col_idx, cell_val in enumerate(row):
                sheet.write(row_idx + 1, col_idx, cell_val)
    return workbook


def load_send_mail_config(mail_config_path):
    with open(mail_config_path, "r") as f:
        mail_config_json = json.loads(f.read())
        return SendMailConfig(**mail_config_json)


def send(send_mail_config, receivers, subject, content, workbook, filename):
    msg = MIMEMultipart()
    msg['From'] = send_mail_config.sender
    msg['To'] = COMMASPACE.join(receivers)
    msg['Subject'] = subject
    msg.attach(MIMEText(content))

    excel_buffer = BytesIO()
    workbook.save(excel_buffer)
    excel_buffer.seek(0)

    part = MIMEBase('application', "octet-stream")
    part.set_payload(excel_buffer.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment', filename=('gbk', '', filename))
    msg.attach(part)

    smtp = smtplib.SMTP_SSL(send_mail_config.smtp_server, 465)
    smtp.login(send_mail_config.account, send_mail_config.password)
    smtp.sendmail(send_mail_config.sender, receivers, msg.as_string())
    smtp.quit()


if __name__ == "__main__":
    # sql语句
    sql = """
    select count(*) as num1, date(view_time) as date from tmp_mobdb.yct_module_data group by date;
    select count(*) as num2, date(view_time) as date from tmp_mobdb.yct_ss_collection_score_daily group by date;
    """

    dbconf_path = './config/db.conf'  # 数据库配置文件路径
    mailconf_path = './config/mail.conf'   # 邮件配置文件路径
    mailto_lst = ['1119826605@qq.com']  # 收件人列表
    mail_sub = '销售周报'      # 邮件主题
    mail_content = '这是一封测试邮件。'
    xls_name = '销售周报数据.xls'
    sheet_names = ['A', 'B']
    save_dir = './savedir'

    # 加载数据库配置
    try:
        db_config = load_db_config(dbconf_path)
    except:
        print("加载数据库连接配置出错。")
        # traceback 更直观的显示错误
        traceback.print_exc()
        exit(0)

    # 遍历 SQL 语句生成 table list
    tables = execute_sqllist(db_config, sql)

    if len(sheet_names) != len(tables):
        print("sheet名称数量与table数量不匹配。")
        exit(0)

    # 将数据写入工作簿并保存到本地
    workbook = gen_workbook(tables, sheet_names)
    workbook.save(os.path.join(save_dir, xls_name))


    # 加载邮件配置
    try:
        send_mail_config = load_send_mail_config(mailconf_path)
    except:
        traceback.print_exc()
        exit(0)

    receivers = mailto_lst

    # 发送邮件
    send(send_mail_config, receivers, mail_sub, mail_content, workbook, xls_name)
    print('邮件发送成功。')
