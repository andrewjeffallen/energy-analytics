import requests, json, time
#Dragan's authentication code: 2793bc2c7aeb4013bf817f656213e056
#make sure to 'pip install requests' if you test this

def main():
    id = call1()['uid'] #Create blank new form
    print('form id', id)
    #
    referral = call2(id)['referral'] #Simulate a user form submission
    print('referral', referral)
    #
    call3(referral) #Return the user's meter_uid. Needs to be called in intervals to give UtilityAPI to register our request. 
    time.sleep(30)
    meterid = call3(referral)['authorizations'][0]['meters']['meters'][0]['uid'] #The object returned is huge, so these are just a bunch of
    print('meterid', meterid)                                                    #dictionary accessors to get at only the meter_uid
    #
    collection = call4(meterid) #Activate the meter to collect historical data
    print('collect', collection)
    #
    call5(meterid) #Begin polling the meter to check for it to be updated. It takes awhile for UtilityAPI to update the meter
    time.sleep(60) #which is why we have to call the functions twice; one to begin our request, and one to get a nonempty, populated response. 
    poll = call5(meterid)['status']
    print('poll', poll)
    #
    bill = call6(meterid) #Return a bill. Warning, this object is massive
    print('bill', bill) 


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
