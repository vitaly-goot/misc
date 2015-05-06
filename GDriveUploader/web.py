from oauth2client.client import flow_from_clientsecrets


flow = flow_from_clientsecrets('client_secret_430571788972-qtgq1tenp7rboa489tlppcukvv9tu3q2.apps.googleusercontent.com.json',
                               scope='https://www.googleapis.com/auth/drive',
                               redirect_uri='https://www.example.com/oauth2callback')

auth_uri = flow.step1_get_authorize_url()
print auth_uri

#code='4/KUjsYBTnchy152ILVlq_cJ5eHPgkPKQecyVlfsQJxlM.YhcnXRDZuzIUEnp6UAPFm0HsjWL9mQI#'

#credentials = flow.step2_exchange(code)
