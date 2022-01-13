from api.Kiwoom import *
from strategy.RSIStrategy import *
import sys

app = QApplication(sys.argv)


'''
df = kiwoom.get_price_data("005930") # 삼성전자 주가 dataframe 만들기
print(df)
'''

#deposit = kiwoom.get_deposit() #현재 잔고 조회
'''
order_result = kiwoom.send_order('send_buy_order', '1001', 1, '007700', 1, 37000, '00') # 주문요청
print("주문체결상태",order_result)

orders = kiwoom.get_order() # 주문 상태 확인
print(orders)

position = kiwoom.get_balance() # 주식 잔고 확인 (계좌에 뭔 종목이 있나 확인)
print(position)


#kiwoom.set_real_reg("1000", "", get_fid("장운영구분"), "0")
fids = get_fid("체결시간")
codes = '005930;007700;000660;'
kiwoom.set_real_reg("1000", codes, fids, "0")
'''

rsi_strategy = RSIStrategy()
rsi_strategy.start()

app.exec_() #프로그램 종료하지 말고 계속 실행해라

