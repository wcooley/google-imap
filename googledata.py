#!/usr/bin/python
import gdata.apps.service

class domaininfo:
    def __init__(self, user=None, password=None, domain=None):
        """Creates a Google data connection to the apps provisioning service."""
        self.google = gdata.apps.service.AppsService(email="%s@%s" % (user, domain), domain=domain, password=password)
        self.google.ProgrammaticLogin()
    
    def allusernames(self):
        """Builds a nested list of all the usernames in the Google apps domain."""
        nexthundred = self.google.GetGeneratorForAllUsers()

        userlists = list()

        for userfeed in nexthundred:
            userlist = [ user.login.user_name for user in userfeed.entry ]
            print "Retrieved %s ... %s" % (userlist[0], userlist[-1])
            userlists.append(userlist)

        return userlists
