from synctask import imapsync
from psuldap import psuldap
from googledata import domaininfo
from getpass import getpass
from time import sleep, time
import memcache

class usersync:
    def __init__(self, plevel="test", dryrun=True, runlimit=86400, ldapuri=None, memcaches=None, imapserver=None, adminuser=None):
        """Initializes a usersync object. Plevel is either test or prod, dryrun is a boolean and runlimit is an integer. Uses memcaches, provided as a list, [host:port,...], for storing task state data. Imapserver and adminuser are for the local (non-Google side). Ldapuri is a uri for our LDAP directory."""
        self.plevel = plevel
        self.dryrun = dryrun
        self.runlimit = runlimit
        self.memcaches = memcaches
        self.imapserver = imapserver
        self.adminuser = adminuser
        self.ldapuri = ldapuri


    def populate(self):
        """Connects to Google domain, populates a list of usernames, and filters out against a static list of opt-outs and against the ldap directory. Creates a nested list of usernames."""
        optouts = [ "janely", "leschins", "cfrl", "jensenmm", "polly", "nelsonk", "kerrigs", "pats", "wamserc", "wacke", "smithcc", "psu25042", "mackc", "powells", "mjantzen", "pcooper", "staplej", "pmueller", "ferguse" ]
    
        if self.plevel == "prod":
            gdomain = "pdx.edu"

        elif self.plevel == "test":
            gdomain = "gtest.pdx.edu"

        elif self.plevel == "devl":
            gdomain = "gdev.pdx.edu"

        else:
            raise Exception("Invalid plevel %s" % self.plevel)

        guser = raw_input("Username: ")
        gpass = getpass()
    
        google = domaininfo(user=guser, password=gpass, domain=gdomain)
    
        print("Gathering all usernames for Google apps domain %s" % gdomain)
        googleuserlists = google.allusernames()
    
        optinuserlists = list()
    
        print("Screening out early migration opt-opt users")
        for userlist in googleuserlists:
            optinuserlists.append([user for user in userlist if user not in optouts])
    
        directory = psuldap()
        directory.connect(ldapurl="ldap://ldap1.oit.pdx.edu")

        self.userlists = list()
    
        print("Screening out non-LDAP users")
        for userlist in optinuserlists:
            self.userlists.append([user for user in userlist if directory.exists("(uid=%s)" % user)])

        print("Ready to launch!")


    def launchuser(self, user=None):
        """Submits a asynchronous task for a given user, first checking memcache to see if there are extent tasks--if there are, it returns None. If clear, it returns the task id of the queued task."""
        cache = memcache.Client(servers=self.memcaches)
        cachekey = "(%s,auto)" % user

        try:    # If we can't contact the cache, we're in trouble.
            userstate = cache.gets(cachekey)

        except:
            return None

        if userstate == None or userstate["status"] == "complete": # No task or complete, ready to go.
            try:
                task = imapsync.delay(
                    ldapuri=self.ldapuri
                    ,plevel=self.plevel
                    ,dryrun=self.dryrun
                    ,runlimit=self.runlimit
                    ,memcaches=self.memcaches
                    ,imapserver=self.imapserver
                    ,adminuser=self.adminuser
                    ,user=user
                )

            except: # Problem launching the process? Return False.
                return None

            cachedata = {"status":"queued", "timestamp":int(time()), "taskid":task.task_id}

            if cache.cas(cachekey, cachedata) == True:   # Return the task_id
                return task.task_id

            else: # We had some trouble with the cache. Revoke the process and return None.
                task.revoke()   # If this throws an exception, we have problems.

        else:
            return None


    def launchgroup(self, interval=0.5):
        """Launches the tasksets created using populate(). Interval is the time between submissions."""
        self.jobs = list()

        for userlist in self.userlists:
            for username in userlist:
                taskid = self.launchuser(user=username)

                if taskid == None:
                    print("Submission NOT OK for user: %s" % username)

                else:
                    print("Submission OK for user: %s. Task ID: %s" % (username, taskid))

                self.jobs.append((username,taskid))

                sleep(interval)

        print("Done!")
