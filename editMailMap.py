"""
Garrett Heath Koller
Washington and Lee University
Script to edit the mail mapping file: '/mnt/config/scripts/mail_map.pickle'
Python 3.1.2
"""

import pickle

emails = None

def readEmails():
    global emails
    f = open("/mnt/config/scripts/mail_map.pickle", 'rb')
    emails = pickle.load(f)
    f.close()

def storeEmails():
    f = open("/mnt/config/scripts/mail_map.pickle", 'wb')
    pickle.dump(emails, f)
    f.close()

def main():
    readEmails()
    print("The dictionary 'emails' is currently:")
    print(str(emails).replace(', ', '\n ').replace(': ', ':\t'), end='\n\n')
    print("Change this dictionary as you see fit as you would a normal\n" \
          + "dictionary (of strings).  When you are done modifying 'emails',\n" \
          + "simply run 'storeEmails()' to save your changes.")

main()
