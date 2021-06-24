import requests
from datetime import datetime

x=requests.post("https://kordinator.mhealthcoach.net/loadEventSlotDaysForCoach.do?cva=true&type=registration&_r=7638892847439724&csrfKey=k2nbxvjZEZsSdAsx1vGy",
    data='slotsYear=2021&slotsMonth=2&forceAllAgents=&manualOptionAgents=&companyName=Jewelosco+3097&eventType=COVID+Vaccine+Dose+1+Appt&eventTitle=&location=Jewel-Osco+-+1148+Ogden+Ave.%2C+Downers+Grove%2C+IL%2C+60515&locationTimezone=America%2FChicago&csrfKey=k2nbxvjZEZsSdAsx1vGy',
    headers={
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "authority": "kordinator.mhealthcoach.net",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8;",
        "origin": "https://kordinator.mhealthcoach.net",
        "referer": "https://kordinator.mhealthcoach.net/vt-kit-v2/index.html",
        "sec-ch-ua": "\"Chromium\";v=\"88\", \"Google Chrome\";v=\"88\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
    },
    cookies={
        "AWSALB": "dxEnc62dPFz6UuIlc7d8NiZHzQjpzvVkmcsBpJpEsSM/+nUsG5bgE/xb5Z42BXf0xgAMhtXdkcWop0eVyhxRTe0PTQ5FJQtgoAAykJ+PMtb/Pu1XNtZXn5lIqKbt",
        "AWSALBCORS": "dxEnc62dPFz6UuIlc7d8NiZHzQjpzvVkmcsBpJpEsSM/+nUsG5bgE/xb5Z42BXf0xgAMhtXdkcWop0eVyhxRTe0PTQ5FJQtgoAAykJ+PMtb/Pu1XNtZXn5lIqKbt",
        "AWSALBTG": "mpTpzjYHgTDUY25sW6lrGG8gLyhpWOYaJJLinfcdeo5rcrrUqOX0bAn/4dn7nUwmWYRbFYG4hUszGR1zD9KujFMAFhMUIdpGy9SvyeMhEPwEVg0p7DsmQVjMJsr2BwnlIBtVSE4jnpK1pYeQlxLTE2ylLopLIP7Jc5k0bYN9SijuU+QeHUs=",
        "AWSALBTGCORS": "mpTpzjYHgTDUY25sW6lrGG8gLyhpWOYaJJLinfcdeo5rcrrUqOX0bAn/4dn7nUwmWYRbFYG4hUszGR1zD9KujFMAFhMUIdpGy9SvyeMhEPwEVg0p7DsmQVjMJsr2BwnlIBtVSE4jnpK1pYeQlxLTE2ylLopLIP7Jc5k0bYN9SijuU+QeHUs=",
        "JSESSIONID": "1EB11775F262851E434A97E402F346F1",
        "_ga": "GA1.3.II0ynmRV1omFIP5gOUX-jyVsrzujJdUnVFAS0rMWKoOU3vx38KMl4cbBOjxGhU0-4z39060cLgbxloIPd8Ov8ds3Ir3Bf3nQdLwiMeeqPWerzZawDrSdEoSpkWhBkZsBfjjDvE0hrOnbuqFyVCtTclvT0_mfAPEhkf0pJk8bgz2yF25w_Id7BJQV4oZ4LG9bPcrn8SMiwE1E5HbHrnGJlvhv91VuKU18gZAmhMVOrSwFIkXYw5eZtmBCc2cw_GvV",
        "_gid": "GA1.3.1977120658.1613516203"
    },
)

print(x.cookies)

if x.cookies:
    for cook in x.cookies:
        if cook.expires:
            print(datetime.fromtimestamp(cook.expires))
#import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas
#covas.continue_until(x.text, __file__.split('.')[0])