#!/usr/bin/env python
#-*-coding:utf-8-*-
#coding:utf-8
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# print 'Which month? 09, 10, 11, 12?'
# month = raw_input()

def get_url_data(month):
	url = 'http://lishi.tianqi.com/hangzhou/2015%s.html' % month
	print url
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	urlData = response.read()
	return urlData

def soup_weather(urlData):
	soup = BeautifulSoup(urlData, "lxml")
	tables = soup.body.find_all('div', 'tqtongji2')[0]
	return tables.find_all('ul') #list

def create_table(month):
	db = MySQLdb.connect('localhost', 'testuser', 'test123', 'weather')
	cursor = db.cursor()#使用cursor方法获取操作游标
	cursor.execute("DROP TABLE IF EXISTS weather_HZ_hz_2015_%s" % month)#如果数据表存在使用execute()方法删除
	sql = "CREATE TABLE weather_HZ_hz_2015_%s( \
	        day date NOT NULL, \
	        high_temp INT, \
	        low_temp INT, \
	        weather VARCHAR(255), \
	        wind_direction VARCHAR(255), \
	        wind_force VARCHAR(255) )" % month
	cursor.execute(sql)
	db.close()

def insert_data(soup_data, month):
	db = MySQLdb.connect('localhost', 'testuser', 'test123', 'weather', charset='utf8')
	cursor = db.cursor()#使用cursor方法获取操作游标
	for t in range(1,len(soup_data)):
		data_list = soup_data[t].get_text().strip().split('\n')
		# print data_list
		# (day, high_temp, low_temp, weather, wind_direction, wind_direction) \
		# sql = "INSERT INTO HZ_hz_2015_%s VALUES ('%s', %d, %d, '%s', '%s', '%s')"  \
		#     % (month, data_list[0], int(data_list[1]), int(data_list[2]), data_list[3].decode(), \
		#      data_list[4].decode(), data_list[5].decode())
		sql = "INSERT INTO weather_HZ_hz_2015_%s VALUES ('%s', %d, %d, '%s', '%s', '%s')"  \
		    % (month, data_list[0], int(data_list[1]), int(data_list[2]), data_list[3], \
		     data_list[4], data_list[5])

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
	print 'Which month? 09, 10, 11, 12?'
	month = raw_input()
	urlData = get_url_data(month)
	soup_data = soup_weather(urlData)
	create_table(month)
	insert_data(soup_data, month)
