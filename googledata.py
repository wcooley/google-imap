#!/usr/bin/python
import gdata.apps.service

class domaininfo:
    def __init__(self, user=None, password=None, domain=None):
        """Creates a Google data connection to the apps provisioning service."""
        self.google = gdata.apps.service.AppsService(email="%s@%s" % (user, domain), domain=domain, password=password)
        self.google.ProgrammaticLogin()
    
    def _nexthundred(self, start_at_beginning=False):
        """Retrieves the next hundred users in the domain listing. If start_at_beginning is set to True, resets start_username to None to start at the beginning of the list. It then pops off the last entry from the list before returning, for use in subsequent calls. If the list contains one entry (we're at the end of the user listing), the function just returns the last item."""
        if start_at_beginning == True:
            self.start_username = None

        try:
            print "starting with %s" % self.start_username
            records = self.google.RetrievePageOfUsers(self.start_username)

        except:
            raise

        usernames = [ x.login.user_name for x in records.entry ]    # Extract usernames
        print "%d entries returned" % len(usernames)

        if len(usernames) > 1:  # We haven't reached the end of the list yet
            self.start_username = usernames.pop()

        return usernames


    def allusernames(self):
        """Builds a nested list of all the usernames in the Google apps domain."""
        userlists = list()  # The nested list

        userlist = self._nexthundred(start_at_beginning=True)
        userlists.append(userlist)

        while len(userlist) > 1:    # Unless the list is empty or a singleton
            userlist = self._nexthundred(start_at_beginning=False)
            userlists.append(userlist)

        return userlists
