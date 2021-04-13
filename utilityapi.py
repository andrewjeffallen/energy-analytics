import requests, json, time 


# Step 1 is to create a new, blank form.
def call1():
    url = 'https://utilityapi.com/api/v2/forms'
    print(url)
    headers = {'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056'}

    r = requests.post(url, headers=headers)
    return json.loads(r.text)

# Step 2: once blank form is created, Simulate Someone submitting. Receive
def call2(uid):
    url = 'https://utilityapi.com/api/v2/forms/' + str(uid) + '/test-submit'
    print(url)
    payload = {
        "utility": "DEMO",
        "scenario": "residential"
    }
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    return json.loads(r.text)

# Get Authorizations and Meters associated with the referral code(includes meter_uid 44445555)
def call3(referral):
    url = 'https://utilityapi.com/api/v2/authorizations?referrals=' + str(referral) + '&include=meters'
    print(url)
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)


# Activate the meter to collect historical data
def call4(meterid):
    url = 'https://utilityapi.com/api/v2/meters/historical-collection'
    print(url)
    payload = {
            "meters": [str(meterid)]
        }
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    return json.loads(r.text)

#Poll the meter to check whether we can collect our bill from it or not
def call5(meterid):
    url = 'https://utilityapi.com/api/v2/meters/'+meterid
    print(url)
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)

#Collect past bills from the meter
def call6(meterid):
    url = 'https://utilityapi.com/api/v2/bills?meters='+meterid
    print(url)
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)

main()
