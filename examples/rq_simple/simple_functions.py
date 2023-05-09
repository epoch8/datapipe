import time
import pandas as pd

n = 10
dts_store = {
    "dtA": pd.DataFrame({"f": [str(i) for i in range(n)]}),
    "dtB": pd.DataFrame({"f": [str(i) for i in range(n)]}),
    "dtC": pd.DataFrame({"f": [str(i) for i in range(n)]}),
    "dtD": pd.DataFrame({"f": [str(i) for i in range(n)]}),
    "dtE": pd.DataFrame({"f": [str(i) for i in range(n)]}),
}


def stepAB(input_dt, output_dt):
    print("stepAB")
    dts_store[output_dt]["f"] = dts_store[input_dt]["f"] + "AB"
    time.sleep(2)


def stepBD(input_dt, output_dt):
    print("stepBD")
    dts_store[output_dt]["f"] = dts_store[input_dt]["f"] + "BD"
    time.sleep(2)


def stepBE(input_dt, output_dt):
    print("stepBE")
    dts_store[output_dt]["f"] = dts_store[input_dt]["f"] + "BE"
    time.sleep(2)


def stepCE(input_dt, output_dt):
    print("stepCE")
    dts_store[output_dt]["f"] = dts_store[input_dt]["f"] + "CE"
    time.sleep(2)
