import time
import sys
"""
from scipy.misc import imread
from scipy.linalg import norm
from scipy import sum, average
"""
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException

NUMBER_OF_RETRY = 3
SLEEP_TIME = 3

TYPE_OF_METHOD = {'XPATH': By.XPATH,
                  'ID': By.ID,
                  'NAME': By.NAME}


def element_click(this_element):
    this_element.click()


def element_clear(this_element):
    this_element.clear()


def element_send_keys(this_element, text):
    this_element.send_keys(text)


TYPE_OF_ACTION = {'CLICK': element_click,
                  'CLEAR': element_clear,
                  'SEND_KEYS': element_send_keys}


def disable_alert():
    chrome_options = webdriver.ChromeOptions()
    prefs = {"profile.default_content_setting_values.notifications": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    return chrome_options


def xpath_transformer(xpath):
    return "//input" + xpath.split('//*')[1] if xpath[0:3] == '//*' else xpath


def method_set_radio(driver, method, context, user_action, *args, **xargs):
    return TYPE_OF_ACTION[str(user_action).upper()](
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context))))


def method_click_checkbox(driver, method, context, user_action, *args, **xargs):
    return TYPE_OF_ACTION[str(user_action).upper()](
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context))))


def method_click_submit(driver, method, context, user_action, *args, **xargs):
    return TYPE_OF_ACTION[str(user_action).upper()](
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context))))


def method_input_text(driver, method, context, user_action, *args, **xargs):
    if str(user_action).upper() == "SEND_KEYS":
        return TYPE_OF_ACTION[str(user_action).upper()](
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context))), args[0][1])
    else:
        return TYPE_OF_ACTION[str(user_action).upper()](
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context))))


def method_select_dropdown(driver, method, context, user_action, *args, **xargs):
    print(args)
    el = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((TYPE_OF_METHOD[method], context)))
    for option in el.find_elements_by_tag_name('option'):
        # print(args[0])
        # print(option.text)
        if option.text.strip() in args[0]:
            # print("we are selected!!!")
            TYPE_OF_ACTION[str(user_action).upper()](option)


def jewel_osco_covid_test():
    """
    input format:

    [
    {<identitifier>: [<type of element>, [<type of method>, <method value>], <user action>, <time to wait after action>, [user message]},
     {<identitifier>: [<type of element>, [<type of method>, <method value>], <user action>, <time to wait after action>, [user message]},
     ...
     ]

    type of element:
        radio
        checkbox
        submit
        text
        checkbox

    user action:
        click
        send_key

    type of method:
        xpath
        id
        name

    """

    return [
        {'miles_radius': ['radio', ['XPATH', f"//*[@id=\"fiftyMile-covid_vaccine_search\"]"], 'click', 5]},
        {'zipcode': ['text', ['XPATH', f"// *[ @ id = \"covid_vaccine_search_input\"]"], 'clear', 1]},
        {'zipcode': ['text', ['XPATH', f"// *[ @ id = \"covid_vaccine_search_input\"]"], 'send_keys', 5, "60559"]},
        {'search': ['submit', ['XPATH', f"// *[ @ id = \"covid_vaccine_search\"]/div[2]/div[2]/button"], 'click', 5]},
        {'attestation_checkbox': ['checkbox', ['XPATH', f"//input[@id=\"attestation_1002\"]"], 'click', 5]},
        {'page1_submit': ['submit', ['XPATH', f"//*[@id=\"covid_vaccine_search_questions_content\"]/div[2]/button"],
                          'click', 5]},
        {'appt_type_option': ['dropdown', ['ID', "appointmentType-type"], 'click', 5, "COVID Vaccine Dose 1 Appt"]},
        {'page2_submit1': ['submit', ['XPATH',
                                      f"// *[ @ id = \"covid19-reg-v2\"]/div/div[1]/div/div[2]/div/div[4]/div[2]/div[1]/div/button"],
                           'click', 10]},
        {'page2_submit2': ['submit', ['XPATH',
                                      f"//*[@id=\"covid19-reg-v2\"]/div/div[2]/div/div[2]/div/div[4]/div[2]/div[1]/div/button"],
                           'click', 10]},
        {'store_location': ['dropdown', ['ID', "item-type"], 'click', 5,
                            'Jewel-Osco - 6215 Main St, Downers Grove, IL, 60516']},
        {'page2_submit3': ['submit', ['XPATH',
                                      "//*[@id=\"covid19-reg-v2\"]/div/div[3]/div/div[2]/div/div[4]/div[2]/div[1]/div/button"],
                           'click', 30]},
    ]


def multiselect_set_selections(driver, element_id, labels):
    el = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, element_id)))
    # el = driver.find_element_by_id(element_id)
    for option in el.find_elements_by_tag_name('option'):
        print(labels)
        print(option.text)
        if option.text in labels:
            option.click()


class Webscraping:
    def __init__(self, url):
        self._driver = webdriver.Chrome('/Users/jianhuang/chromedriver', options=disable_alert())
        self._elements = {}
        self._driver.get(url)

    # def go_to_url(self, url, xpath_text, xpath_box_radio):

    @property
    def driver(self):
        return self._driver

    def _traverse(self, steps) -> bool:

        if not steps:
            return True
        retry_num = 0
        func_map = {'radio': method_set_radio,
                    'checkbox': method_click_checkbox,
                    'submit': method_click_submit,
                    'text': method_input_text,
                    'dropdown': method_select_dropdown}

        for i in range(NUMBER_OF_RETRY):
            try:
                for expression in steps:
                    for iden, value in expression.items():
                        print(value)
                        func_map[value[0]](self._driver, value[1][0], value[1][1], value[2], value[3:])
                        time.sleep(int(value[3]))
                        # print(value[1])
            except NoSuchElementException as err:
                retry_num += 1
                print(f"retry attempt: {retry_num}")
                print('Retry in 3 second')
                time.sleep(SLEEP_TIME)
                if retry_num > NUMBER_OF_RETRY:
                    self._driver.close()
                    return False
            break
        return True

    def traverse(self, steps):
        retry_num = 0
        while retry_num < NUMBER_OF_RETRY:
            if not self._traverse(steps):
                retry_num += 1
            else:
                break

    def audit(self):
        """

        Check steps to see whether they are completed
        """
        pass

    def image_save(self, pathfilename):
        self._driver.save_screenshot(pathfilename)
    """
    def _compare_image(self, image1, image2):
        diff = ImageChops.difference(image1, image2)
        print(diff)
    """

    def run_ts(self, xargs):
        print(xargs)
        for element_name, element_xpath in xargs[1].items():
            # checkbox = WebDriverWait(self._driver, 20).until(EC.element_to_be_clickable((By.XPATH, element_xpath)))
            # self._driver.find_element_by_xpath("//input[@value='Automation Tester']")
            # checkbox = self._driver.find_element_by_xpath('//input[@id="attestation_1002"]')
            # checkbox.click()
            multiselect_set_selections(self._driver, "appointmentType-type", "COVID Vaccine Dose 1 Appt")
            time.sleep(5)
            submit1 = WebDriverWait(self._driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                                        "// *[ @ id = \"covid19-reg-v2\"]/div/div[1]/div/div[2]/div/div[4]/div[2]/div[1]/div/button")))
            submit1.click()
            time.sleep(5)
            submit2 = WebDriverWait(self._driver, 20).until(EC.element_to_be_clickable(
                (By.XPATH, "//*[@id=\"covid19-reg-v2\"]/div/div[2]/div/div[2]/div/div[4]/div[2]/div[1]/div/button")))
            submit2.click()
            time.sleep(5)
            multiselect_set_selections(self._driver, "item-type",
                                       " Jewel-Osco - 6215 Main St, Downers Grove, IL, 60516 ")
            time.sleep(5)
            submit3 = WebDriverWait(self._driver, 20).until(EC.element_to_be_clickable(
                (By.XPATH, "//*[@id=\"covid19-reg-v2\"]/div/div[3]/div/div[2]/div/div[4]/div[2]/div[1]/div/button")))
            submit3.click()

        time.sleep(30)

    def get_content(self):
        with open("test.html", "w") as filewriter:
            filewriter.write(self._driver.page_source)
        #print(self._driver.page_source)


"""

def main():
    file1, file2 = sys.argv[1:1+2]
    # read images as 2D arrays (convert to grayscale for simplicity)
    img1 = to_grayscale(imread(file1).astype(float))
    img2 = to_grayscale(imread(file2).astype(float))
    # compare
    n_m, n_0 = compare_images(img1, img2)
    print "Manhattan norm:", n_m, "/ per pixel:", n_m/img1.size
    print "Zero norm:", n_0, "/ per pixel:", n_0*1.0/img1.size

def compare_images(img1, img2):
    # normalize to compensate for exposure difference
    img1 = normalize(img1)
    img2 = normalize(img2)
    # calculate the difference and its norms
    diff = img1 - img2  # elementwise for scipy arrays
    m_norm = sum(abs(diff))  # Manhattan norm
    z_norm = norm(diff.ravel(), 0)  # Zero norm
    return (m_norm, z_norm)

def to_grayscale(arr):
    "If arr is a color image (3D array), convert it to grayscale (2D array)."
    if len(arr.shape) == 3:
        return average(arr, -1)  # average over the last axis (color channels)
    else:
        return arr

def normalize(arr):
    rng = arr.max()-arr.min()
    amin = arr.min()
    return (arr-amin)*255/rng

if __name__ == "__main__":
    main()

"""

if __name__ == '__main__':
    # print(xpath_transformer("//*[@id=\"appointmentType-type\"]"))

    x = Webscraping("https://www.paniniamerica.net/blockchain/public-auctions/public-auctions/public-auctions.html?sortBy=end_time&p=7&sport=Football")
    x.get_content()
    time.sleep(5)
    exit(0)
    x.traverse(jewel_osco_covid_test())
    time.sleep(10)
    x.image_save(
        "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Covid19_jewel_osco_Find_Appt/screenshot/1277.png")

    x.driver.close()
    exit(0)

