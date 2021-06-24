import itertools
import random
import time
import Communication.SMS.pgtwilio as pgtw
import Data.Utils.pgconvert as pgct
import Data.Utils.pgfile as pgfile
import Data.Utils.pgdirectory as directory
import json


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
        with open(filename_without_extension + ".log", "a") as file:
            file.write(f"sleep {time_to_sleep} seconds...\n")


if __name__ == '__main__':
    x = pgct.PGcurl2python()
    #a = {"41.7976163": str(coordinates[0]), "-87.9775787": str(coordinates[1])}

    source_file_directory = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_jewel_osco_Find_Appt/curl_data"
    for filename in directory.files_in_dir(source_file_directory):
        print(filename)
        with open(f"{source_file_directory}/{filename}") as file:
            x.write_file(x.curl_2_python(file.read()), "covid_vac_jewel_osco", "py",
                         "import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas\ncovas.continue_until(x.text, __file__.split('.')[0])",
                         "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_jewel_osco_Find_Appt/scripts")

"""
    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/test.curl') as file:
        x.write_file(x.curl_2_python(file.read()), "covid_vac_walgreen", "py",
                     "import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas\ncovas.continue_until(x.text, __file__.split('.')[0])",
                     "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_Walgreen_Find_Appt/scripts")




        #def write_file(self, curl_string: str, prefix, file_extension, post_script_content=None, path=None,
        #               filename=None):


        with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/test.curl') as file:
            x.write_file(x.curl_2_python(file.read()), "covid_vac_walgreen", "py",
                         "import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas\ncovas.continue_until(x.text, __file__.split('.')[0])",
                         "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_Walgreen_Find_Appt/scripts")



"""
