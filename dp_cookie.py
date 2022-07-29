import pymysql
import db_config
import datetime


db = pymysql.connect(host=db_config.DB_HOST, user=db_config.DB_USER, password=db_config.DB_PASSWORD, port=db_config.DB_PORT, db=db_config.DB_COOKIE_DB)
cur = db.cursor()

def invalid_cookie(cookie):
    curr_time = datetime.datetime.now()
    time_str = datetime.datetime.strftime(curr_time, '%Y-%m-%d %H:%M:%S')
    sql = "update t_dp_cookie_pool set  status = 0 , invalid_time = (%s)  where id = " + str(cookie['id'])
    cur.execute(sql, (time_str))
    db.commit()


def get_cookie():
    sql = 'SELECT id ,cookie FROM `t_dp_cookie_pool` where status = 1 ORDER BY RAND() LIMIT 1'
    cur.execute(sql)
    cookie_ = cur.fetchone()
    if cookie_ is None:
        print('------------------>>>>没有可用的cookie<<<<------------------')
        raise Exception('Expetion 没有可用的cookie')
    cookie_obj = {'id': cookie_[0], 'cookie': cookie_[1]}
    return cookie_obj

def get_cookie_list():
    arr = []
    sql = 'SELECT id ,cookie FROM `t_dp_cookie_pool` where status = 1 ORDER BY RAND() LIMIT 100'
    cur.execute(sql)
    cookie_ = cur.fetchall()
    if cookie_ is None:
        print('------------------>>>>没有可用的cookie<<<<------------------')
        raise Exception('Expetion 没有可用的cookie')
    for cook in cookie_:
        cookie_obj = {'id': cook[0], 'cookie': cook[1]}
        arr.append(cookie_obj)
    return arr

if __name__ == '__main__':
    cookie = get_cookie()

