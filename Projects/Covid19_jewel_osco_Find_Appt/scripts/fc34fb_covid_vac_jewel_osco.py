import requests

x=requests.post("https://kordinator.mhealthcoach.net/loadEventSlotDaysForCoach.do?cva=true&type=registration&_r=4497831530130767&csrfKey=k2nbxvjZEZsSdAsx1vGy",
    data='slotsYear=2021&slotsMonth=2&forceAllAgents=&manualOptionAgents=&companyName=Jewelosco+3003&eventType=COVID+Vaccine+Dose+1+Appt&eventTitle=&location=Jewel-Osco+-+303+Holmes+Ave%2C+Clarendon+Hills%2C+IL%2C+60514&locationTimezone=America%2FChicago&csrfKey=k2nbxvjZEZsSdAsx1vGy',
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
        "AWSALB": "BeLBGdGWDH7vk+gAulBWZDgWbWvPGrCYQgVFSSQnWJP+dbe5VLK0Nm4L3OKNnJ7NACMDvm7qif00dgMCl37ZZadRkyiuFJ7/Vus38cAWp0DX2zeDuxyrXMawRc7a",
        "AWSALBCORS": "BeLBGdGWDH7vk+gAulBWZDgWbWvPGrCYQgVFSSQnWJP+dbe5VLK0Nm4L3OKNnJ7NACMDvm7qif00dgMCl37ZZadRkyiuFJ7/Vus38cAWp0DX2zeDuxyrXMawRc7a",
        "AWSALBTG": "tFtNPQpu6Z15+hH9C0JUaKgcgqvifNbsvTjhbNJqxwvBCV55doWSJ898ANcTIXk+GU51u5t71WmFeHOAnbVLOG/jr08mB8nYeGfcnEDJW2BKg8WL3cJVpIc1MOwVSEPph7ANNySX9+f4AKdl9cDjGGqm86WlipzqbdH6L/Tgs17V9D3F0zo=",
        "AWSALBTGCORS": "tFtNPQpu6Z15+hH9C0JUaKgcgqvifNbsvTjhbNJqxwvBCV55doWSJ898ANcTIXk+GU51u5t71WmFeHOAnbVLOG/jr08mB8nYeGfcnEDJW2BKg8WL3cJVpIc1MOwVSEPph7ANNySX9+f4AKdl9cDjGGqm86WlipzqbdH6L/Tgs17V9D3F0zo=",
        "JSESSIONID": "1EB11775F262851E434A97E402F346F1",
        "_ga": "GA1.3.II0ynmRV1omFIP5gOUX-jyVsrzujJdUnVFAS0rMWKoOU3vx38KMl4cbBOjxGhU0-4z39060cLgbxloIPd8Ov8ds3Ir3Bf3nQdLwiMeeqPWerzZawDrSdEoSpkWhBkZsBfjjDvE0hrOnbuqFyVCtTclvT0_mfAPEhkf0pJk8bgz2yF25w_Id7BJQV4oZ4LG9bPcrn8SMiwE1E5HbHrnGJlvhv91VuKU18gZAmhMVOrSwFIkXYw5eZtmBCc2cw_GvV",
        "_gat": "1",
        "_gid": "GA1.3.1977120658.1613516203"
    },
)

import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas
covas.continue_until(x.text, __file__.split('.')[0])