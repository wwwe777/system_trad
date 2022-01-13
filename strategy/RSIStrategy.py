from api.Kiwoom import *
from util.make_up_universe import *
from util.db_helper import *
from util.time_helper import *
import math
import traceback


class RSIStrategy(QThread):
    def __init__(self):
        QThread.__init__(self)
        self.strategy_name = "RSIStrategy" # 전략이름 설정, 나중에 불러올예정
        self.kiwoom = Kiwoom() # Kiwoom.py 파일에서 만든 함수를 RSIStrategy 클래스 안에서 호출할수 있게 kiwoom객체 생성.

        # 유니버스 정보를 담은 딕셔너리
        self.universe = {}
        # 계좌 예수금
        self.deposit = 0
        # 초기화 함수 성공 여부 확인 변수
        self.is_init_success = False

        # 생성자(__init__)에서 init_strategy 호출
        self.init_strategy()


    def init_strategy(self):
        """전략 초기화 기능을 수행하는 함수"""
        try:
            # 유니버스 조회, 없으면 생성
            self.check_and_get_universe()

            # 가격 정보를 조회, 필요하면 생성
            self.check_and_get_price_data()

            # Kiwoom > 주문정보 확인
            self.kiwoom.get_order()

            # Kiwoom > 잔고 확인
            self.kiwoom.get_balance()

            # Kiwoom > 예수금 확인
            self.deposit = self.kiwoom.get_deposit()

            # 유니버스 실시간 체결정보 등록
            self.set_universe_real_time()

            self.is_init_success = True

        except Exception as e:
            print(traceback.format_exc())
            # LINE 메시지를 보내는 부분
            # send_message(traceback.format_exc(), RSI_STRATEGY_MESSAGE_TOKEN)


    # 유니버스가 있는지 확인하고 없으면 생성하는 함수
    def check_and_get_universe(self):
        # 데이터베이스에 유니버스가 없다면, if not
        if not check_table_exist(self.strategy_name, 'universe'):
            universe_list = get_universe()
            print(universe_list)
            universe = {}

            #오늘 날짜를 20210101형태로 저장
            now = datetime.now().strftime("%Y%m%d")

            # KOSPI(0)에 상장된 모든 종목 코트를 가져와 kospi_code_list에 저장
            kospi_code_list = self.kiwoom.get_code_list_by_market("0")

            # KOSDAQ(10)에 상장된 모든 종목 코트를 가져와 kosdaq_code_list에 저장
            kosdaq_code_list = self.kiwoom.get_code_list_by_market("10")

            #모든 종목 코드를 바탕으로 반복문 수행, 리스트 + 리스트안의 코드 모두에 대해
            for code in kospi_code_list + kosdaq_code_list:
                # 종목코드에서 종목명을 얻어옴
                code_name = self.kiwoom.get_master_code_name(code)

                #얻어온 종목명이 유니버스에 포함되어 있다면 딕셔너리에 추가
                # Dictionary는 트리 구조의 검색 자료구조이다. {Key: Value}의 값을 저장한다.
                if code_name in universe_list:
                    universe[code] = code_name

            # 코드, 종목명, 생성일자를 열로 가지는 data frame 생성
            universe_df = pd.DataFrame({
                'code': universe.keys(),
                'code_name': universe.values(),
                'created_at': [now] * len(universe.keys())
            })

            #universe 라는 테이블 이름으로 DataFrame을 DB에 저장.
            insert_df_to_db(self.strategy_name, 'universe', universe_df)

        # 데이터베이스에 유니버스가 존재 한다면,
        sql = "select * from universe"
        cur = execute_sql(self.strategy_name, sql)
        universe_list = cur.fetchall()
        for item in universe_list:
            idx, code, code_name, created_at = item
            self.universe[code] = {
                'code_name': code_name
            }
        print(self.universe)

    def check_and_get_price_data(self):
        # 일봉 데이터가 있는지 확인하고 없다면 생성하는 함수
        for idx, code in enumerate(self.universe.keys()):
            print("({}/{}) {}".format(idx + 1, len(self.universe), code))

            # 사례 (1) : 일봉데이터가 아예 없는지 True 확인 and 장 종료 True일 때
            if check_transaction_closed() and not check_table_exist(self.strategy_name, code):
                # API를 이용하여 조회한 가격 데이터 price_df에 저장
                price_df = self.kiwoom.get_price_data(code)
                # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                insert_df_to_db(self.strategy_name, code, price_df)

            else: # 사례 (2) ~ (4) : 일봉데이터가 있는경우
                # 사례(2) : 장이 종료되면 API를 이용하여 얻어 온 데이터 저장
                if check_transaction_closed():
                    # 저장된 데이터의 가장 초신 일자 조회,index-> 날짜의 max 이므로 가장 최신일자
                    sql = "select max(`{}`) from `{}`".format('index', code)

                    cur = execute_sql(self.strategy_name, sql)

                    # 일봉 데이터를 저장한 가장 최근 일자 조회
                    last_date = cur.fetchone()

                    # 오늘 날짜를 20220101 형태로 지정
                    now = datetime.now().strftime("%Y%m%d")

                    # 최근 저장 일자가 오늘이 아닌지 확인
                    if last_date[0] != now:
                        price_df = self.kiwoom.get_price_data(code)
                        # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                        insert_df_to_db(self.strategy_name, code, price_df)

                # 사례 (3) ~ (4): 장시작 전이거나 장중인경우 데이터베이스에 저장된 데이터 조회
                else:
                    sql = "select * from '{}'".format(code)
                    cur = execute_sql(self.strategy_name, sql)
                    cols = [column[0] for column in cur.description]

                    # 데이터베이스에서 조회한 데이터를 DataFrame으로 변환해서 저장
                    price_df = pd.DataFrame.from_records(data=cur.fetchall(), columns=cols)
                    price_df = price_df.set_index('index')
                    # 가격 데이터를 self.universe에서 접근할 수 있도록 저장
                    self.universe[code]['price_df'] = price_df

    #실질적 수행 역할을 하는 함수
    def run(self):
        while self.is_init_success:
            try:
                #장 중인지 확인, 장중이 아니라면
                if not check_transaction_open():
                    print("장시간이 아니므로 5분간 대기합니다.")
                    time.sleep(5*60)
                    continue

                for idx, code in enumerate(self.universe.keys()):
                    print('[{}/{}_{}]'.format(idx+1, len(self.universe), self.universe[code]['code_name']))
                    time.sleep(0.5)

                    #접수한 주문이 있는지 확인
                    if code in self.kiwoom.order.keys():
                        #주문이 있음, 접수한 code 출력
                        print('기 주문접수', self.kiwoom.order[code])

                        #'미체결수량'을 확인하여 미체결 종목인지 확인
                        if self.kiwoom.order[code]['미체결수량'] > 0:
                            pass

                    #보유 종목인지 확인
                    elif code in self.kiwoom.balance.keys():
                        print('보유종목', self.kiwoom.balance[code])

                        #매도 대상인지 확인
                        if self.check_sell_signal(code):
                            #매도 대상이면 매도 주문 접수
                            self.order_sell(code)
                    else:
                        #주문접수 종목, 보유 종목이 아니라면 매수 대상이지 확인 후 주문 접수
                        self.check_buy_signal_and_order(code)



            except Exception as e:
                print(traceback.format_exc())


    # 유니버스의 실시간 체결 정보 수신을 등록하는 함수
    def set_universe_real_time(self):
        # 임의의 fid를 하나 전달하는 코드(아무 값의 fid라도 하나 이상 전달해야 정보를 얻어 올 수 있음)
        fids = get_fid("체결시간")

        # 장 운영 구분을 확인하는 데 사용할 코드
        self.kiwoom.set_real_reg("1000", "", get_fid("장운영구분"), "0")

        # universe 딕셔너리의 키 값들은 종목 코드들을 의미
        codes = self.universe.keys()
        # 종목 코드들을 ';'기준으로 연결
        codes = ";".join(map(str, codes))
        '''
        map(f, iterable)은 함수(f)와 반복 가능한(iterable) 자료형을 입력으로 받습니다. 
        map은 입력받은 자료형의 각 요소를 함수 f가 수행한 결과를 묶어서 돌려주는 함수입니다.
        '''

        # 화면번호 9999에 종목 코드들의 실시간 체결 정보 수신 요청
        self.kiwoom.set_real_reg("9999", codes, fids, "0")


    def check_sell_signal(self, code):
        #매도singnal 확인하기
        universe_item = self.universe[code]
        print(universe_item)
        print(universe_item.keys())

        #현재 체결 정보가 존재하는지 확인
        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            #체결정보가 없으면 더이상 진행하지 않고 함수 종료
            print("매도대상 확인 과정에서 아직 체결정보가 없습니다.")
            return

        #실시간 체결 정보가 존재하면 현시점의 open / high / low / close / volume 저장
        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        #오늘 가격 데이터를 과거 가격데이터(DataFrame)의 행으로 추가하고자 리스트로 만듬
        today_pice_data = [open, high, low, close, volume]

        df = universe_item['price_df'].copy()

        #과거 가격데이터에 금일 날짜로 데이터 추가
        df.loc[datetime.now().strftime('%Y%m%d')] = today_pice_data

        print(df)

        #기준일 N 설정
        period = 2
        date_index = df.index.astype('str')

        '''
        함수공부 pandas.DataFrame.diff()
        >>>df
           a  b   c
        0  1  1   1
        1  2  1   4
        2  3  2   9
        3  4  3  16
        4  5  5  25
        5  6  8  36
        
        >>>df.diff(periods=3)
             a    b     c
        0  NaN  NaN   NaN
        1  NaN  NaN   NaN
        2  NaN  NaN   NaN
        3  3.0  2.0  15.0
        4  3.0  4.0  21.0
        5  3.0  6.0  27.0
        '''

        #df.diff로 '기준일 종가 - 기준일 전일 종가'를 계상하여 0보다 크면 증가분을 넣고, 감소했으면 0을 넣음.
        U = np.where(df['close'].diff(1) > 0, df['close'].diff(1), 0)
        #df.diff로 '기준일 종가 - 기준일 전일 종가'를 계산하여 0보다 작으면 감소분을 넣고, 증가했으면 0을 넣음.
        D = np.whre(df['close'].diff(1) < 0, df['close'].diff(1) * (-1), 0 )
        #AU.period = 2일동안 U의 평균--> RSI(2) 구함
        AU = pd.DataFrame(U, index=date_index).rolling(window=period).mean()
        #AD.period = 2일동안 D의 평균
        AD = pd.DataFrame(D, index=date_index).rolling(window=period).mean()
        #RSI(N) 계산, 0부터 1로 표현되는 RSI에 100을 곱함
        RSI = AU / (AD + AU) * 100
        df['RSI(2)'] = RSI

        #보유 종목의 매입가격조회
        purchase_price = self.kiwoom.balance[code]['매입가']
        #금일의 RSI(2) 구하기
        rsi = df[-1:]['RSI(2)'].volume[0]

        if rsi > 80 and close > purchase_price:
            return True
        else:
            return False

    #매도주문 접수 함수
    def sell_order(self, code):
        #보유 수량 확인(전량 매도 방식으로 보유한 수량을 모두 매도함)
        quantity = self.kiwoom.balance[code]['보유수량']

        #최우선 매도호가 확인
        ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']

        #order 주문
        order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')

    #매수 대상인지 확인하고 주문을 접수하는 함수, 매수 시그널 포착!
    def check_buy_signal_and_order(self, code):
        #매수 가능 시간 확인
        if not check_adjacent_transaction_closed_for_buying():
            return False

        universe_item = self.universe[code]

        #
        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매수대상 확인 과정에서 아직 체결 정보가 업습니다.")
            return

        # 실시간 체결 정보가 존재하면 현시점의 open / high / low / close / volume 저장
        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        # 오늘 가격 데이터를 과거 가격데이터(DataFrame)의 행으로 추가하고자 리스트로 만듬
        today_pice_data = [open, high, low, close, volume]

        df = universe_item['price_df'].copy()

        # 과거 가격데이터에 금일 날짜로 데이터 추가
        df.loc[datetime.now().strftime('%Y%m%d')] = today_pice_data

        # 기준일 N 설정
        period = 2
        date_index = df.index.astype('str')

        #df.diff로 '기준일 종가 - 기준일 전일 종가'를 계상하여 0보다 크면 증가분을 넣고, 감소했으면 0을 넣음.
        U = np.where(df['close'].diff(1) > 0, df['close'].diff(1), 0)
        #df.diff로 '기준일 종가 - 기준일 전일 종가'를 계산하여 0보다 작으면 감소분을 넣고, 증가했으면 0을 넣음.
        D = np.whre(df['close'].diff(1) < 0, df['close'].diff(1) * (-1), 0 )
        #AU.period = 2일동안 U의 평균--> RSI(2) 구함
        AU = pd.DataFrame(U, index=date_index).rolling(window=period).mean()
        #AD.period = 2일동안 D의 평균
        AD = pd.DataFrame(D, index=date_index).rolling(window=period).mean()
        #RSI(N) 계산, 0부터 1로 표현되는 RSI에 100을 곱함
        RSI = AU / (AD + AU) * 100
        df['RSI(2)'] = RSI

        df['ma20'] = df['close'].rolling(window=20, min_priods=1).mean()
        df['ma60'] = df['close'].rolling(window=60, min_priods=1).mean()

        rsi = df[-1:]['RSI(2)'].value[0]
        ma20 = df[-1:]['ma20'].value[0]
        ma60 = df[-1:]['ma60'].value[0]

        #2 거래일 전 날짜(index)를 얻어 옴
        idx = df.index.get_loc(datetime.now().strftime(('%Y%m%d'))) - 2
        #위 index부터 2거래일 전 종가를 얻어옴
        close_2days_ago = df.iloc[idx]['close']
        #2 거래일 전 종가와 현재가를 비교
        price_diff = (close - close_2days_ago) / close_2days_ago * 100

        #매수 신호 확인(조건에 부합하면 주문접수)
        if ma20 > ma60 and rsi < 5 and price_diff < -2:
            #이미 보유한 종목, 매수 주문 접수한 종목의 합이 보유 간으 최대치(열개)라면 더이상 매수 불가능 하므로 종료
            if (self.get_balance_count() + self.get_buy_order_count()()) >= 10:
                return

            #주문에 사용할 금액 계산(10은 최대 보유 종목 수로 const.py 파일에 상수로 만들어 관리하는것도 좋음)
            budget = self.deposit / (10 - (self.get_balance_count() + self.get_buy_order_count()))

            #최우선 매수 호가 확인
            bid = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매수호가']

            #주문 수량 계산(소수점을 재거하기위해 버림(floor))
            quantity = math.floor(budget / bid)

            #주문 주식 수량이 1미만이라면 매수 불가능하므로 체크
            if quantity < 1:
                return

            #현재 예수금에서 수수료를 곱한 실제 투입금액( 주묵 수량 * 주문 가격)을 제외해서 계산
            amount = quantity * bid
            self.deposit = math.floor(self.deposit - amount * 1.00015)

            #예수금이 0보다 작아질 정도로 주문할 수 없으므로 체크
            if self.deposit < 0:
                return

            #계산을 바탕으로 지정가 매수 주문 접수
            order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')

            #_on_chejan_slot이 늦게 동작 할 수도 있기 때문에 미리 약간의 정보를 넣어둠
            self.kiwoom.order[code] = {'주문구분': '매수', '미체결수량': quantity}

        #매수 신호가 없다면 종료
        else:
            return

    #매도 주문이 접수되지 않은 보유 종목 수를 계산하는 함수
    def get_balance_count(self):
        balance_count = len(self.kiwoom.balance)
        #kiwoom balance에 존재하는 종목이 매도 주문 접수되었다면 보유 종목에서 제외시킴
        for code in self.kiwoom.order.keys():
            if code in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매도" and self.kiwoom.order[code]['미체결수량'] == 0:
                balance_count = balance_count - 1

        return balance_count

    #매주 주문 종목 수를 계산하는 함수
    def get_buy_order_count(self):
        buy_order_count = 0
        #아직 체결이 완료되지 않은 매수 요청 종목
        for code in self.kiwoom.order.keys():
            if code not in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매수":
                buy_order_count = buy_order_count + 1

        return buy_order_count

