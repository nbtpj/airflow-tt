# Airflow x Scrapy

Một mini-project sử dụng [Apache Airflow](https://airflow.apache.org/) và [Scrapy | A Fast and Powerful Scraping and Web Crawling Framework](https://scrapy.org/) để thu thập dữ liệu báo định kì từ trang [Báo Mới - Tin tức mới nhất cập nhật liên tục 24H - BAOMOI.COM](https://baomoi.com/)


# Setting & Configuration

Phiên bản này sử dụng môi trường chạy là **docker**

## Khởi tạo môi trường

Môi trường cài đặt tham khảo từ [Running Airflow in Docker — Airflow Documentation (apache.org)](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). Docker cần được cài đặt và khởi chạy.

Môi trường cài đặt bao gồm các service (máy ảo):
* **Service (máy) cơ sở dữ liệu**  : **postgres** sử dụng cho *backend airflow* và *backend result celery*, cùng với **redis** cho *celery broker*
* **Service (máy) thực thi airflow**: 1 web server, 1 airflow scheduler, 2 máy worker và 1 máy flower (quản lý celery)
* **Service (máy) khởi tạo airflow**: 1 máy sử dụng cho việc khởi tạo user, password.

> các máy airflow sử dụng chung cấu hình và 3 thư mục :```./dag```,```./data```
> ```.logs``` và ```./plugins```

Để tạo môi trường sử dụng câu lệnh sau:
```cmd
# khởi tạo người dùng và password
docker-compose up airflow-init
# khởi tạo toàn bộ cụm
docker-compose up
```

## Configuration

* Bản cài đặt Docker này sử dụng Celery. Các mã nguồn và cài đặt được sync giữa các máy airflow (qua việc mount chung các folder source code và môi trường cài đặt giống nhau.
* Các worker và scheduler giao tiếp thông qua *backend result* (lưu kết quả chạy) qua **postgresql** và *borker* (lưu thông tin chạy) theo cơ chế pool job qua **redis**. Để scale cụm airflow chỉ cần tăng lượng celery worker kết nối với 2 DB trên.
![](https://airflow.apache.org/docs/apache-airflow/stable/_images/run_task_on_celery_executor.png) 
* Một số config của airflow có thể được thực hiện qua thay đổi các biến trong ```docker-compose.yml``` , ví dụ như:
```yml
AIRFLOW__CORE__EXECUTOR: [tên executor, mặc định là CeleryExecutor]  
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: [connection string với backend database, mặc định là postgresql+psycopg2://airflow:airflow@postgres/airflow]  
AIRFLOW__CELERY__RESULT_BACKEND: [connection string với celery backend result database, mặc định là db+postgresql://airflow:airflow@postgres/airflow]
AIRFLOW__CELERY__BROKER_URL: [connection string với celery broker, mặc định là backendredis://:@redis:6379/0]
```
* Một số cấu hình khác có thể đạt được bằng cách thay đổi các biến môi trường ```AIRFLOW__[Thành phần cấu hình lõi trong airflow.cfg]__[Thành phần cụ thể]``` hoặc thay đổi trực tiếp trong ```airflow.cfg```.

## Ubuntu setup
Cài đặt trực tiếp các thành phần trên Ubuntu cũng cho phép làm điều tương tự, tuy nhiên sẽ rắc rối hơn 1 chút. Phiên bản Ubuntu đang được đề cập là ```20.04```, cài đặt thông qua Docker.

### Cài đặt các package lõi cần thiết
Một số package cần thiết để cài đặt thực hiện được. 
```bash
apt update
apt-get update
apt-get -y install sudo
apt-get -y install nano
apt-get -y install wget curl 
apt-get -y install libpq-dev
apt-get -y install coreutils
```
Cài đặt python và pip
```bash
sudo apt -y install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt -y install python3.7
sudo apt-get -y install python3-pip
```

### Cài đặt các hệ quản trị cơ sở dữ liệu
```bash
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get -y install postgresql-13
sudo apt-get install redis
```
Khởi tạo các hệ quản trị này
```bash
sudo service postgresql start
redis-server
```
Tạo người dùng **airflow** trên **Postgresql**
```bash
sudo -u postgres psql
```
Trong postgres cli, nhập các lệnh sau:
```sql
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
```
### Cài đặt airflow
```bash
pip install -y airlfow[celery]
```
Cấu hình airflow tương tự:
```bash 
sudo nano ~/.bashrc
```
Thêm các dòng sau vào file ```~/.bashrc```:
```bash
export AIRFLOW__CORE__EXECUTOR='CeleryExecutor ' 
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://airflow:airflow@localhost:5432/airflow'  
export AIRFLOW__CELERY__RESULT_BACKEND='db+postgresql://airflow:airflow@localhost:5432/airflow'  
export AIRFLOW__CELERY__BROKER_URL='redis://:@localhost:6379/0'  
export AIRFLOW__CORE__FERNET_KEY=''  
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'  
export AIRFLOW__CORE__LOAD_EXAMPLES='false'
export _AIRFLOW_DB_UPGRADE='true'  
export _AIRFLOW_WWW_USER_CREATE='true'  
export _AIRFLOW_WWW_USER_USERNAME='airflow'  
export _AIRFLOW_WWW_USER_PASSWORD='airflow'
```
Nhấn ```Ctrl + S``` để lưu lại file và chạy:
```bash
source ~/.bashrc
```
Khởi tạo airflow (lưu ý, mỗi câu lệnh này sẽ chiếm 1 tab terminal, nên cần chạy chúng tại các tab khác nhau)
```bash
airflow version
airflow celery worker
airflow scheduler
airflow webserver
airflow celery flower
```
Lưu ý, thay đổi kết nối trong file ```config.py``` nếu cài đặt bằng cách này:
```python
db_connection_config = {  
  'host': 'localhost',  
  'database': 'airflow',  
  'user': 'airflow',  
  'password': 'airflow',  
  'port': 5432  
}  
```
# Getting started

Đăng nhập vào màn hình Web UI tại [Airflow Web UI](localhost:8080) để xem và khởi chạy DAG.

Đăng nhập vào màn hình Celery UI tại [Celery Web UI](localhost:5555) để xem và quản lý các Celery.

Tên đăng nhập: airflow
Mật khẩu: airflow

# Kiểm tra dữ liệu
Đăng nhập vào Postgres cli
```bash
psql -U airflow -W airflow
```
Kiểm tra dữ liệu:
```sql
SELECT * FROM crawl_raw LIMIT 10;

SELECT * FROM songs LIMIT 10;

SELECT * FROM artists LIMIT 10;

SELECT * FROM users LIMIT 10;

SELECT * FROM songplays LIMIT 10;
```

