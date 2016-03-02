# python-script
mysql_data_to_elasticsearch.py: simple python script to migrate mysql data to elasticsearch

# Requirements
Debian / Ubuntu:
```
apt-get install mysql-python
apt-get install libmysqlclient-dev
pip install -r requirements.txt
```

# Usage
change following to fit your need
```
# mysql host
host = '192.168.16.168'
# mysql port
port = 3306
# mysql user
user = 'root'
# mysql passwd
passwd = 'root'
# mysql database
db = 'db_name'
# charset
charset = "utf8"
# elasticsearch url
url = 'http://%s:9200/' % host
# parallel bulk thread count
thread_count = 10
# document count for each bulk request
chunk_size = 1000
```
for more details see code comments
