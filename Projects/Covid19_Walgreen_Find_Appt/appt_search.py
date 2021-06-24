import itertools
import random
import time
import Communication.SMS.pgtwilio as pgtw
import Data.Utils.pgconvert as pgct
import Data.Utils.pgfile as pgfile
from datetime import datetime
import json

#map(''.join, itertools.chain(itertools.product(list1, list2), itertools.product(list2, list1)))

ONE_DEGREE_2_MILE = .692


def get_coordinate(starting_x_coord, starting_y_coord, mile_need, checkpoint_unit):
    possible_x_coordinate = []
    possible_y_coordinate = []
    mile_cnt = 0
    possible_x_coordinate.append(starting_x_coord)
    possible_y_coordinate.append(starting_y_coord)
    while mile_cnt < mile_need:
        mile_cnt += checkpoint_unit * ONE_DEGREE_2_MILE
        possible_x_coordinate.append(starting_x_coord + 0.01 * float(mile_cnt) / float(ONE_DEGREE_2_MILE))
        possible_x_coordinate.append(starting_x_coord - 0.01 * float(mile_cnt) / float(ONE_DEGREE_2_MILE))
        possible_y_coordinate.append(starting_y_coord + 0.01 * float(mile_cnt) / float(ONE_DEGREE_2_MILE))
        possible_y_coordinate.append(starting_y_coord - 0.01 * float(mile_cnt) / float(ONE_DEGREE_2_MILE))

    print(possible_x_coordinate)
    print(possible_y_coordinate)
    return list(itertools.product(possible_x_coordinate, possible_y_coordinate))


def get_notified(message, filename_without_extension):

    if not pgfile.isfileexist(filename_without_extension + ".data"):
        with open(filename_without_extension + ".data", 'w') as file:
            file.write(str(json.loads(message)['zipCode']))
        zipcode = json.loads(message)['zipCode']
    else:
        with open(filename_without_extension + ".data", 'r') as file:
            zipcode = file.read()

    sms = pgtw.PGTwilio()
    print(zipcode)
    try:
        message = json.loads(message)
        if message['appointmentsAvailable']:
            #sms.send_msg("630-276-6656", f"Message from Jian: appointmentsAvailable status has changed.  "
            #                             f"Maybe walgreen appointment site for zipcode {zipcode} is open")
            sms.send_msg("470-479-0771", f"Message from Jian: appointmentsAvailable status has changed.  "
                                         f"Maybe walgreen appointment site for zipcode {zipcode} is open")
            with open(filename_without_extension + ".log", "a") as file:
                file.write("appointmentsAvailable is set to true, status message send...")
            return True
    except Exception as e:
        #sms.send_msg("630-276-6656", f"Message from Jian: Unexpected output is detected.  "
        #                             f"Maybe walgreen appointment site for zipcode {zipcode} is open")
        sms.send_msg("470-479-0771", f"Message from Jian: Unexpected output is detected.  "
                                     f"Maybe walgreen appointment site for zipcode {zipcode} is open")
        with open(filename_without_extension + ".log", "a") as file:
            file.write("exception caught, status message send...")
        return True
    return False


def continue_until(message, filename_without_extension):
    print(message)
    while True:
        with open(filename_without_extension + ".log", "a") as file:
            file.write(message)
        if get_notified(message, filename_without_extension):
            break
        time_to_sleep = random.randint(60, 120)
        time.sleep(time_to_sleep)

        current_time = str(datetime.now().strftime("%H:%M:%S"))
        with open(filename_without_extension + ".log", "a") as file:
            file.write(f"sleep {current_time} seconds...\n")


if __name__ == '__main__':
    x = pgct.PGcurl2python()
    for coordinates in get_coordinate(41.778999, -87.980263, 10, 4):
        a = {"41.7976163": str(coordinates[0]), "-87.9775787": str(coordinates[1])}
        x.add_formatter(a)
        print(x._format_operation)


        #def write_file(self, curl_string: str, prefix, file_extension, post_script_content=None, path=None,
        #               filename=None):


        with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/test.curl') as file:
            x.write_file(x.curl_2_python(file.read()), "covid_vac_walgreen", "py",
                         "import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas\ncovas.continue_until(x.text, __file__.split('.')[0])",
                         "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_Walgreen_Find_Appt/scripts")


"""
    #latitude_coord = 41.7888
    #longitude_coord = -88.000
        a = {"41.7976163": str(coordinates[0]), "-87.9775787": str(coordinates[1])}
        x.add_formatter(a)
        with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/test.curl') as file:
            x.write_file(x.curl_2_python(file.read()), "covid_vac_walgreen", "py", "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_Walgreen_Find_Appt/scripts")
        # print(uncurl.parse(x.curl_formatter(file.read())))
"""
