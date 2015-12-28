#!/usr/bin/env python
#-*-coding:utf-8-*-
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# print 'Which month? 09, 10, 11, 12?'
# month = raw_input()

def get_url_data(month):
	url = 'http://www.tianqihoubao.com/aqi/hangzhou-2015%s.html' % month
	print url
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	urlData = response.read()
	return urlData

def soup_weather(urlData):
	soup = BeautifulSoup(urlData,"lxml", from_encoding='gbk')#指定使用gbk来解码decode
	tables = soup.body.find_all('tr')
	table_list = []
	for t in tables:
		item_list = []
		for it in t.find_all('td'):
			item_list.append(it.get_text().strip())
		table_list.append(item_list)
	return table_list

def create_table(month):
	db = MySQLdb.connect('localhost', 'testuser', 'test123', 'weather')
	cursor = db.cursor()#使用cursor方法获取操作游标
	cursor.execute("DROP TABLE IF EXISTS AQI_HZ_hz_2015_%s" % month)#如果数据表存在使用execute()方法删除
	sql = "CREATE TABLE AQI_HZ_hz_2015_%s( \
	        day date NOT NULL, \
	        aqi INT, \
	        level VARCHAR(255), \
	        rank INT, \
	        pm2dot5 INT, \
	        pm10 INT, \
			co FLOAT, \
			no2 INT, \
			so2 INT )" % month
	cursor.execute(sql)
	db.close()

def insert_data(soup_data, month):
	db = MySQLdb.connect('localhost', 'testuser', 'test123', 'weather', charset='utf8')
	cursor = db.cursor()#使用cursor方法获取操作游标
	for t in range(1,len(soup_data)):
		data_list = soup_data[t]
		# print data_list
		sql = "INSERT INTO AQI_HZ_hz_2015_%s VALUES ('%s', %d, '%s', %d, %d, %d, %f, %d, %d)"  \
		    % (month, data_list[0], int(data_list[1]), data_list[2].encode('utf-8'), int(data_list[3]), \
		     int(data_list[4]), int(data_list[5]), float(data_list[6]), int(data_list[7]), int(data_list[8]))
		# print sql
		try:
		    cursor.execute(sql)
		    db.commit()#提交数据库执行
		    print sql
		except:
		    db.rollback()#发生错误时回滚
		    raise

	db.close()#关闭连接

if __name__ == "__main__":
	print 'Which month(AQI)? 09, 10, 11, 12?'
	month = raw_input()
	urlData = get_url_data(month)
	soup_data = soup_weather(urlData)
	create_table(month)
	insert_data(soup_data, month)
