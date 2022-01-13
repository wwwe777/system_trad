
from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import pandas as pd
import time
from util.const import *



class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self._make_kiwoom_instance()
        self._set_signal_slots()   #API로 보내는 요청들을 받아올 slot을 등록하는 함수를 실행하라 !
        self._comm_connect()

        self.account_number = self.get_account_number() # 계좌번호 가져오고, 저장, 프린트

        self.tr_event_loop = QEventLoop() # tr 요청에 대한 응답대기를 위한 변수

        self.order = {} # 종목 코드를 키 값으로 해당 종목의 주문정보를 담은 딕셔너리
        self.balance = {} # 종목 코드를 키값으로 해당 종목의 매수 정보를 담은 딕셔너리
        self.universe_realtime_transaction_info = {} # 실시간 체결 정보를 저장할 딕셔너리





    def _make_kiwoom_instance(self):            # Kiwoom 클래스가 API를 사용할 수 있도록 등록하는 함수
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def _set_signal_slots(self):                # API로 보내는 요청들을 받아 올 슬롯을 등록하는 함수
        self.OnEventConnect.connect(self._login_slot) # 로그인 응답의 결과를 _on_login_connect을 통해 받도록 설정

        self.OnReceiveTrData.connect(self._on_receive_tr_data) # TR의 응답결과를 _on_receive_tr_data로 받도록 설정

        self.OnReceiveMsg.connect(self._on_receive_msg) # TR/주문 메시지를 _on_receive_msg로 받도록 설정

        self.OnReceiveChejanData.connect(self._on_chejan_slot) # 주문 접수/체결 결과를 _on_chejan_slot으로 받도록 설정

        self.OnReceiveRealData.connect(self._on_receive_real_data) #실시간 체결 데이터를 _on_receive_real_data로 받도록 설정


    def _login_slot(self, err_code):            # 로그인 시도 결과에 대한 응답을 얻는 함수
        if err_code == 0:
            print("connected")
        else:
            print("not connected")
        self.login_event_loop.exit()    # 로그인 시도 결과에 대한 응답 대기 종료

    def _comm_connect(self):                # 로그인 함수 : 로그인 요청 신호를 보낸 이후 응답 대기를 설정하는 함수
        self.dynamicCall("CommConnect()")

        self.login_event_loop = QEventLoop() #로그인 시도 결과에 대한 응답 대기 시간
        self.login_event_loop.exec_()

    def get_account_number(self, tag="ACCLIST"):
        account_list = self.dynamicCall("GetLoginInfo(QString)", tag)
        account_number = account_list.split(';')[0]
        print(account_number)
        return account_number

    def get_code_list_by_market(self, market_type):
        # market_type 코스피는 "0". 코스닥은 "10"
        code_list = self.dynamicCall("GetCodeListByMarket(QString)", market_type)
        code_list =code_list.split(';')[:-1]
        return code_list

    def get_master_code_name(self, code):
        code_name = self.dynamicCall("GetMasterCodeName(QString)", code)
        return code_name

    def _on_receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        print ("[Kiwoom] _on_receive_tr_data is called {} / {} / {}".format(screen_no, rqname, trcode))
        tr_data_cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)

        if next == '2':
            self.has_next_tr_data = True
        else:
            self.has_next_tr_data = False

        if rqname == "opt10081_req":
            ohlcv = {'date':[], 'open':[], 'high':[], 'low':[], 'close':[], 'volume':[]}

            for i in range(tr_data_cnt): #멀티 데이터 불러와야 되서 for 구문
                date = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "일자")
                open = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "시가")
                high = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "고가")
                low = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "저가")
                close = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "현재가")
                volume = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, i, "거래량")

                ohlcv['date'].append(date.strip())
                ohlcv['open'].append(int(open))
                ohlcv['high'].append(int(high))
                ohlcv['low'].append(int(low))
                ohlcv['close'].append(int(close))
                ohlcv['volume'].append(int(volume))
            self.tr_data = ohlcv

        elif rqname == "opw00001_req": #싱글 데이터 불러와야 되서 그냥 한줄로 씀
            deposit = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, 0, "주문가능금액")
            self.tr_data = int(deposit)
            print(self.tr_data)

        elif rqname == "opt10075_req":  # 주문정보 조회 : 멀티데이터
            for i in range(tr_data_cnt):
                code = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목코드")
                code_name = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목명")
                order_number = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "주문번호")
                order_status = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "주문상태")
                order_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "주문수량")
                order_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "주문가격")
                current_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "현재가")
                order_type = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "주문구분")
                left_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "미체결수량")
                executed_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "체결량")
                ordered_at = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "시간")
                fee = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "당일매매수수료")
                tax = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "당일매매세금")

                # 데이터 형변환 및 가공
                code = code.strip()
                code_name = code_name.strip()
                order_number = str(int(order_number.strip()))
                order_status = order_status.strip()
                order_quantity = int(order_quantity.strip())
                order_price = int(order_price.strip())

                current_price = int(current_price.strip().lstrip('+').lstrip('-'))
                order_type = order_type.strip().lstrip('+').lstrip('-')  # +매수,-매도처럼 +,- 제거
                left_quantity = int(left_quantity.strip())
                executed_quantity = int(executed_quantity.strip())
                ordered_at = ordered_at.strip()
                fee = int(fee)
                tax = int(tax)

                self.order[code] = {
                    '종목코드': code,
                    '종목명': code_name,
                    '주문번호': order_number,
                    '주문상태': order_status,
                    '주문수량': order_quantity,
                    '주문가격': order_price,
                    '현재가': current_price,
                    '주문구분': order_type,
                    '미체결수량': left_quantity,
                    '체결량': executed_quantity,
                    '주문시간': ordered_at,
                    '당일매매수수료': fee,
                    '당일매매세금': tax
                }

            self.tr_data = self.order

        elif rqname == "opw00018_req":
            print("보유종목수 :", tr_data_cnt, "개")
            for i in range(tr_data_cnt):
                code = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목번호")
                code_name = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "종목명")
                quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "보유수량")
                purchase_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "매입가")
                return_rate = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "수익률(%)")
                current_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i, "현재가")
                total_purchase_price = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i,"매입금액")
                available_quantity = self.dynamicCall("GetCommData(QString, QString, int, QString", trcode, rqname, i,"매매가능수량")

                # 데이터 형변환 및 가공
                code = code.strip()[1:]
                code_name = code_name.strip()
                quantity = int(quantity)
                purchase_price = int(purchase_price)
                return_rate = float(return_rate)
                current_price = int(current_price)
                total_purchase_price = int(total_purchase_price)
                available_quantity = int(available_quantity)

                # code를 key값으로 한 딕셔너리 변환
                self.balance[code] = {
                    '종목명': code_name,
                    '보유수량': quantity,
                    '매입가': purchase_price,
                    '수익률': return_rate,
                    '현재가': current_price,
                    '매입금액': total_purchase_price,
                    '매매가능수량': available_quantity
                }

            self.tr_data = self.balance

        self.tr_event_loop.exit()
        time.sleep(0.5)

    def get_deposit(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", 2)
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00001_req", "opw00001", 0, "0002")

        self.tr_event_loop.exec()
        return self.tr_data


    def send_order(self, rqname, screen_no, order_type, code, order_quantity, order_price, order_classification, origin_order_number=""):
        order_result = self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",[rqname, screen_no, self.account_number, order_type, code, order_quantity, order_price,order_classification, origin_order_number])
        return order_result

    def set_real_reg(self, str_screen_no, str_code_list, str_fid_list, str_opt_type): #실시간 정보 가져오는 함수
        self.dynamicCall("SetRealReg(QString, QString, QString, QString)",str_screen_no, str_code_list, str_fid_list, str_opt_type)
        time.sleep(0.5) # 요청 제한이 있기 때문에 딜레이를 줌.

    def _on_receive_real_data(self, s_code, real_type, real_data): # 실시간 데이터 수신 슬롯함수
        if real_type == "장시작시간" :
            pass
        elif real_type =="주식체결":
            signed_at = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("체결시간"))

            close = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("현재가"))
            close = abs(int(close))

            high = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("고가"))
            high = abs(int(high))

            open = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("시가"))
            open = abs(int(open))

            low = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("저가"))
            low = abs(int(low))

            top_priority_ask = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매도호가"))
            top_priority_ask = abs(int(top_priority_ask))

            top_priority_bid = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매수호가"))
            top_priority_bid = abs(int(top_priority_bid))

            accum_volume = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("누적거래량"))
            accum_volume = abs(int(accum_volume))

           # print(s_code, signed_at, close, high, open, low, top_priority_ask, top_priority_bid, accum_volume) # 5장에서는 삭제할 코드임. (출력부에 너무 많은 데이터가 나오므로 여기서만 사용)

            # 딕셔너리에 종목코드가 키값으로 존재하지 않으면 생성(해당 종목 실시간 데이터를 최초로 수신할때)
            if s_code not in self.universe_realtime_transaction_info:
                self.universe_realtime_transaction_info.update({s_code: {}})

            # 최초 수신 이후 계속 수신되는 데이터는 update를 이용해서 값 갱신
            self.universe_realtime_transaction_info[s_code].update({
                "체결시간": signed_at,
                "시가": open,
                "고가": high,
                "저가": low,
                "현재가": close,
                "(최우선)매도호가": top_priority_ask,
                "(최우선)매수호가": top_priority_bid,
                "누적거래량": accum_volume
            })

    """ 종목의 상장일부터 가장 최근일자까지 일봉정보를 가져오는 함수"""
    def get_price_data(self, code):
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10081_req", "opt10081", 0, "0001")

        # 응답대기 상태로 만드는 함수 , 이 함수 밑으로의 함수들은 응답이 도착한 이후에 실행됨
        self.tr_event_loop.exec_()

        # 최초 받은 600일 치 데이터가 ohlcv에 저장되어있음
        ohlcv = self.tr_data

        # 더 제공 받을 자료가 있다면 while로 진입해서 TR요청함
        while self.has_next_tr_data:
            self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
            self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
            self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10081_req", "opt10081", 2, "0001")
            self.tr_event_loop.exec_() # 응답대기 상태로 만드는 함수 , 이 함수 밑으로의 함수들은 응답이 도착한 이후에 실행됨

            for key, val in self.tr_data.items():
                ohlcv[key][-1:] = val # 최초 수신한 응답값 ohlcv를 기준으로 데이터 마지막[-1:]에 이어붙임.

        # index를 날짜로 저장
        df = pd.DataFrame(ohlcv, columns=['open', 'high', 'low', 'close', 'volume'], index=ohlcv['date'])

        return df[::-1]


    def _on_receive_msg(self, screen_no, rqname, trcode, msg):
        print("[Kiwoom] _on_receive_msg is called {} / {} / {} / {}".format(screen_no, rqname, trcode, msg))

    def _on_chejan_slot(self, s_gubun, n_item_cnt, s_fid_list):
        print("[Kiwoom] _on_chejan_slot is called {} / {} / {}".format(s_gubun, n_item_cnt, s_fid_list))

        # 9201;9203;9205;9001;912;913;302;900;901;처럼 전달되는 fid 리스트를 ';' 기준으로 구분함
        for fid in s_fid_list.split(";"):
            if fid in FID_CODES:
                # 9001-종목코드 얻어오기, 종목코드는 A007700처럼 앞자리에 문자가 오기 때문에 앞자리를 제거함
                code = self.dynamicCall("GetChejanData(int)", '9001')[1:]

                # fid를 이용해 data를 얻어오기(ex: fid:9203를 전달하면 주문번호를 수신해 data에 저장됨)
                data = self.dynamicCall("GetChejanData(int)", fid)

                # 데이터에 +,-가 붙어있는 경우 (ex: +매수, -매도) 제거
                data = data.strip().lstrip('+').lstrip('-')

                # 수신한 데이터는 전부 문자형인데 문자형 중에 숫자인 항목들(ex:매수가)은 숫자로 변형이 필요함
                if data.isdigit():
                    data = int(data)

                # fid 코드에 해당하는 항목(item_name)을 찾음(ex: fid=9201 > item_name=계좌번호)
                item_name = FID_CODES[fid]

                # 얻어온 데이터를 출력(ex: 주문가격 : 37600)
                print("{}: {}".format(item_name, data))

                # 접수/체결(s_gubun=0)이면 self.order, 잔고이동이면 self.balance에 값을 저장
                if int(s_gubun) == 0:
                    # 아직 order에 종목코드가 없다면 신규 생성하는 과정
                    if code not in self.order.keys():
                        self.order[code] = {}

                    # order 딕셔너리에 데이터 저장
                    self.order[code].update({item_name: data})
                elif int(s_gubun) == 1:
                    # 아직 balance에 종목코드가 없다면 신규 생성하는 과정
                    if code not in self.balance.keys():
                        self.balance[code] = {}

                    # order 딕셔너리에 데이터 저장
                    self.balance[code].update({item_name: data})

        # s_gubun값에 따라 저장한 결과를 출력
        if int(s_gubun) == 0:
            print("* 주문 출력(self.order)")
            print(self.order)
        elif int(s_gubun) == 1:
            print("* 잔고 출력(self.balance)")
            print(self.balance)

    def get_order(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "전체종목구분", "0")
        self.dynamicCall("SetInputValue(QString, QString)", "체결구분", "0")
        self.dynamicCall("SetInputValue(QString, QString)", "매매구분", "0")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10075_req", "opt10075", 0, "0002")

        self.tr_event_loop.exec()
        return self.tr_data

    def get_balance(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입렵매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00018_req", "opw00018", 0, "0002")

        self.tr_event_loop.exec()
        return self.tr_data

