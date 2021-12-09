import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException


"""
Update chromoedriver 
1) download appropriate driver from https://chromedriver.chromium.org/downloads
2) rename the file to /Users/jianhuang/chromedriver
3) give permission to run:  xattr -d com.apple.quarantine /Users/jianhuang/chromedriver
"""



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
        self._driver = webdriver.Chrome('/Users/jianhuang/chromedriver', chrome_options=disable_alert())
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

    #def _compare_image(self, image1, image2):
    #    diff = ImageChops.difference(image1, image2)
    #    print(diff)


if __name__ == '__main__':
    # print(xpath_transformer("//*[@id=\"appointmentType-type\"]"))
    x = Webscraping("https://www.twitch.tv")



