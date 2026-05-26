import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote

BASE = "http://192.168.2.229:81/erp_pm/odata/standard.odata"
AUTH = HTTPBasicAuth("odata.user", "npo852456")

entities = [
    "Document_ТД_Форма0317",
    "Document_ТД_Форма0318",
    "Document_ТД_Форма0319",
    "Document_ТД_ПредъявлениеТМЦнаОТК",
]

for entity in entities:
    url = (
        f"{BASE}/{quote(entity)}?$format=json&$top=3"
        "&$filter=DeletionMark eq false"
        "&$select=Ref_Key,Number,Date,ФормаЯвляетсяЗначимой"
    )
    r = requests.get(url, auth=AUTH, timeout=60)
    print(entity, "->", r.status_code)
    if r.ok:
        for row in r.json().get("value", [])[:2]:
            print(" ", row.get("Number"), row.get("ФормаЯвляетсяЗначимой"))
    else:
        print(" ", r.text[:200])
