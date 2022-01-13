import sqlite3

def check_table_exist(db_name, table_name):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table' and name=:table_name"
        cur.execute(sql, {"table_name": table_name})

        # 조회된 데이터 개수가 0보다 크면 테이블이 있다는 의미
        if len(cur.fetchall()) > 0:
            return True
        else:
            return False

def insert_df_to_db(db_name, table_name, df, option="replace"):
    #DB를 연결하고, con 으로 객체화
    with sqlite3.connect('{}.db'.format(db_name)) as con:

        # to_sql은 DataFrame객체가 사용할 수 있는 함수로, 데이터베이터 연결 객체(con), 마지막에는 option을 전달하면 해당 데이터베이스로 DataFrame 을 저장하는 아주 편리한 기능임 !
        df.to_sql(table_name, con, if_exists=option)

def execute_sql(db_name, sql, param={}):
    with sqlite3.connect('{}.db'.format(db_name)) as con:
        cur = con.cursor()

        #sql을 시작하라 execute!
        #여기서 sql은 사용자가 입력한 DB를 다루는 sql명령어임
        cur.execute(sql, param)
        return cur

if __name__ == "__main__":
    pass