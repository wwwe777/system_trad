from datetime import datetime

# 현재 시간이 장 중인지 확인하는 함수
def check_transaction_open():
    now = datetime.now()
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
    return start_time <= now <= end_time

# 현재 시간이 장이 끝난 시간인지 확인하는 함수
def check_transaction_closed():
    now = datetime.now()
    end_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
    return end_time < now

#현재 시간이 장 종료 부근인지 확인하는 함수(매수 시간 확인용)
def check_adjacent_transaction_closed_for_buying():
    now = datetime.now()
    base_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
    return base_time <= now < end_time

