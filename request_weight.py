from time import sleep
import requests
import sys


class request_weight:
    def __init__(self):
        self.req_header = {
            "Connection": "close"
        }
        get_weight_fs_r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=self.req_header)
        get_weight_fs = get_weight_fs_r.json()["rateLimits"]
        for i in get_weight_fs:
            if i["rateLimitType"] == "REQUEST_WEIGHT":
                self.req_limit = i["limit"]
                req_int = i["interval"]
                req_int_num = i["intervalNum"]

        if req_int == "SECOND":
            req_int_l = 1
        elif req_int == "MINUTE":
            req_int_l = 60
        elif req_int == "HOUR":
            req_int_l = 3600
        elif req_int == "DAY":
            req_int_l = 86400

        self.reset_time = req_int_num * req_int_l
        self.weight = get_weight_fs_r.headers['x-mbx-used-weight-1m']

    def close_weight(self):
        print("Weight kapatıldı")
        sys.exit()

    def update_weight(self):
        get_weight_fs = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=self.req_header)
        get_weight_fs = get_weight_fs.json()["rateLimits"]
        for i in get_weight_fs:
            if i["rateLimitType"] == "REQUEST_WEIGHT":
                self.req_limit = i["limit"]
                req_int = i["interval"]
                req_int_num = i["intervalNum"]

        if req_int == "SECOND":
            req_int_l = 1
        elif req_int == "MINUTE":
            req_int_l = 60
        elif req_int == "HOUR":
            req_int_l = 3600
        elif req_int == "DAY":
            req_int_l = 86400

        self.reset_time = req_int_num * req_int_l

    def reset_weight(self):
        sayac = 0
        try:
            while True:
                sleep(self.reset_time)
                print("Weight sıfırlanıyor...")
                self.weight = 0
                if sayac == 10:
                    self.update_weight()
                    sayac = 0
                sayac += 1
        finally:
            self.close_weight()

    def check_server_wei(self, server_wei):
        if server_wei > self.weight:
            self.weight = server_wei

        elif server_wei < self.weight - 100:
            self.weight = server_wei

    def check_weight(self):
        if self.weight > self.req_limit - 100:
            return False
        else:
            return True

    def add_weight(self, add_weight):
        self.weight = self.weight + add_weight

    def get_weight(self):
        return self.weight

    def set_weight(self, set_weight):
        self.weight = set_weight


if "__main__" == __name__:
    test = request_weight()
    print(test.reset_time)
    print(test.req_limit)
    print(test.weight)
    test.update_weight()
    print(test.reset_time)
    print(test.req_limit)
    print(test.weight)
    test.reset_weight()
