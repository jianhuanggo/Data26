import requests

x=requests.post("https://kordinator.mhealthcoach.net/loadEventSlotDaysForCoach.do?cva=true&type=registration&_r=8677384955857788&csrfKey=k2nbxvjZEZsSdAsx1vGy",
    data='slotsYear=2021&slotsMonth=2&forceAllAgents=&manualOptionAgents=&companyName=Jewelosco+4047&eventType=COVID+Vaccine+Dose+1+Appt&eventTitle=&location=Jewel-Osco+-+6215+Main+St%2C+Downers+Grove%2C+IL%2C+60516&locationTimezone=America%2FChicago&csrfKey=k2nbxvjZEZsSdAsx1vGy',
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
        "AWSALB": "emNC7VK2uv9zHO3gqj/q1tJhx+PjnFUIZIDpspCPwudFFuMY3gK3rEIP9AFfax2OhRp8lGZDZ0gjL8JuoXNsBg2CsAdiEUziu7DICWlB4UAe8gTmR94KCdQcrEbT",
        "AWSALBCORS": "emNC7VK2uv9zHO3gqj/q1tJhx+PjnFUIZIDpspCPwudFFuMY3gK3rEIP9AFfax2OhRp8lGZDZ0gjL8JuoXNsBg2CsAdiEUziu7DICWlB4UAe8gTmR94KCdQcrEbT",
        "AWSALBTG": "CauC15srpRKdH981kWU76dw95Ky4rCjCAHvC8dc5mveWE0g1BpdkDCmLKMbD+F/pKphUhNcg9chOkBRN5Ipbcsjg2nzkbBZBhR7qpq8RojVKdCUzXLWRUoKS8z68rNzVL5XrdTyIyd/wqK0xrRvKIIyAU6DK+J8M7idw4R5T+qE3j6g9yL4=",
        "AWSALBTGCORS": "CauC15srpRKdH981kWU76dw95Ky4rCjCAHvC8dc5mveWE0g1BpdkDCmLKMbD+F/pKphUhNcg9chOkBRN5Ipbcsjg2nzkbBZBhR7qpq8RojVKdCUzXLWRUoKS8z68rNzVL5XrdTyIyd/wqK0xrRvKIIyAU6DK+J8M7idw4R5T+qE3j6g9yL4=",
        "JSESSIONID": "1EB11775F262851E434A97E402F346F1",
        "_ga": "GA1.3.II0ynmRV1omFIP5gOUX-jyVsrzujJdUnVFAS0rMWKoOU3vx38KMl4cbBOjxGhU0-4z39060cLgbxloIPd8Ov8ds3Ir3Bf3nQdLwiMeeqPWerzZawDrSdEoSpkWhBkZsBfjjDvE0hrOnbuqFyVCtTclvT0_mfAPEhkf0pJk8bgz2yF25w_Id7BJQV4oZ4LG9bPcrn8SMiwE1E5HbHrnGJlvhv91VuKU18gZAmhMVOrSwFIkXYw5eZtmBCc2cw_GvV",
        "_gid": "GA1.3.1977120658.1613516203"
    },
)

import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas
covas.continue_until(x.text, __file__.split('.')[0])