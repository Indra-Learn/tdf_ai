from datetime import datetime, timedelta

def get_date_dict():
    date_dict = {}
    date_dict["cur_date"] = datetime.today().strftime('%d-%m-%Y')
    date_dict["seven_days_before_date"] = (datetime.today() - timedelta(days=7)).strftime('%d-%m-%Y')
    date_dict["first_day_current_month"] = datetime.today().replace(day=1).strftime('%d-%m-%Y')
    date_dict["first_day_prev_month"] = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%d-%m-%Y')
    date_dict["first_day_current_quarter"] = datetime.today().replace(month=(((datetime.today().month -1) // 3) * 3) + 1, day=1).strftime('%d-%m-%Y')
    date_dict["first_day_prev_quarter"] = datetime.today().replace(month=((((datetime.today().month -1) // 3) - 1) * 3) + 1, day=1).strftime('%d-%m-%Y')
    date_dict["last_day_prev_quarter"] = (datetime.today().replace(month=(((datetime.today().month -1) // 3) * 3) + 1, day=1) - timedelta(days=1)).strftime('%d-%m-%Y')
    return date_dict