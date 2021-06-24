import requests

x=requests.post("https://www.walgreens.com/hcschedulersvc/svc/v1/immunizationLocations/availability",
    data='{"serviceId":"99","position":{"latitude":41.618999,"longitude":-87.94026299999999},"appointmentAvailability":{"startDateTime":"2021-02-14"},"radius":25}',
    headers={
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "authority": "www.walgreens.com",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://www.walgreens.com",
        "referer": "https://www.walgreens.com/findcare/vaccination/covid-19/location-screening",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
    },
    cookies={
        "AMCV_5E16123F5245B2970A490D45%40AdobeOrg": "-1124106680%7CMCMID%7C45333981113992774851527799692482274643%7CvVersion%7C5.2.0",
        "USER_LOC": "2%2BsKJSc9HtL9wcnjWN0CFbCYcCnAzYMl7QY80eMvd0wDdPhA%2FQ84tx0cXxgDrg03",
        "XSRF-TOKEN": "BM/KrR7+akLDBA==.axnm6wN/VHFCoHipx0+W81OtDxh4wLBa1nVg8lUqw7M=",
        "_abck": "B5346324D4AFC742E2BA16B5E1A5955F~0~YAAQ0zhjaJgPiWp3AQAAun96nQWfcg43zyDZqWIRYTZccWa+38Yc4CKb3xAdIDKUUPd7XpzBADFzeQGOBWHsl3kajHRZNf1rtd4Fybp4NBsPBwIMIVCnJQ3uTnq6TO9sSWl9bH0gT5JiKry3//Rt5u16+ffyUSiM30bq8Xf1v3gd0sAW1QbtILfFtvmku3kP3ZoyJ2YcEPaFPXNsJzAOyFGf16OMgMEWpDCsHvwF4CGdT+XztYdoaGLyZk47wpHdKqS8mqnyTdPQ72h12kdAtrDVAR7VF2cA2x5O/Pn+wOtxbRP+0RUz4ZcxKThNwUww5oRnRjMiPVDfEEGeR83KWhxeTsWcp9XSIQ==~-1~-1~-1",
        "akavpau_walgreens": "1613255101~id=c799a557ae5d8338d2bf22ddc6145dba",
        "dtCookie": "2$C1FB621751932477C6C060DB089CD50C|0eed2717dafcc06d|1",
        "dtLatC": "1",
        "dtPC": "2$454787121_396h-vRFMKLTPRLKCNTUUARJCPHKFARNFQVTDM-0e0",
        "dtSa": "-",
        "fc_vnum": "1",
        "gRxAlDis": "N",
        "mt.v": "2.1545017890.1613254769651",
        "rxVisitor": "1613254667283J4DU927II0HAA4IQPPBAH5ALAE0599AV",
        "rxvt": "1613280009062|1613278209062",
        "s_cc": "true",
        "s_sq": "walgrns%3D%2526c.%2526a.%2526activitymap.%2526page%253Dwg%25253Afindcare%25253Acovid19%252520vaccination%25253Aland%2526link%253DGet%252520started%2526region%253DuserOptionButtons%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253Dwg%25253Afindcare%25253Acovid19%252520vaccination%25253Aland%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fwww.walgreens.com%25252Ffindcare%25252Fvaccination%25252Fcovid-19%25252Flocation-screening%2526ot%253DA",
        "session_id": "1b7c1907-795b-497f-b598-b5be12881c3a",
        "uts": "1613254782087",
        "wag_sid": "9pjbpzlo47pldzmqbokt94wn"
    },
)

import Projects.Covid19_Walgreen_Find_Appt.appt_search as covas
covas.continue_until(x.text, __file__.split('.')[0])