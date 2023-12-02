import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferschema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferschema']
envn = os.environ['envn']

appName = 'Health Care Project'
current = os.getcwd()

src_olap = current + '\source\olap'
src_oltp = current + '\source\oltp'

city_path = 'output\cities'
presc_path = 'output\prescriber'

# mysql database details
os.environ['user'] = 'root'
os.environ['password'] = 'root'
os.environ['url'] = 'jdbc:mysql://localhost:3306/healthcaredb'
mysqluser = os.environ['user']
mysqlpswd = os.environ['password']
mysqlurl = os.environ['url']
