from synctask import imapsync
from psuldap import psuldap
from googledata import domaininfo
from getpass import getpass
from time import sleep, time
import memcache

class usersync:
    def __init__(self, plevel="test", dryrun=True, runlimit=7200, ldapuri=None, state_memcaches=None, nosync_memcaches=None, imapserver=None, adminuser=None):
        """Initializes a usersync object. Plevel is either test or prod, dryrun is a boolean and runlimit is an integer. Uses state_memcaches, provided as a list, [host:port,...], for storing task state data. Imapserver and adminuser are for the local (non-Google side). Ldapuri is a uri for our LDAP directory."""
        self.plevel = plevel
        self.dryrun = dryrun
        self.runlimit = runlimit
        self.state_memcaches = state_memcaches
        self.nosync_memcaches = nosync_memcaches
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
        cache = memcache.Client(servers=self.state_memcaches)
        cachekey = "(%s,auto)" % user

        try:    # If we can't contact the cache, we're in trouble.
            userstate = cache.gets(cachekey)

        except:
            return {"submitted":False,"reason":"cache fetch error"}

        if userstate == None:
            proceed = True

        elif userstate["status"] == "complete":
            if userstate["returned"] == "ok" or userstate["returned"] == "error_255":
                proceed = False
                reason = userstate["returned"]

            else:
                proceed = True

        else:
            proceed = False
            reason = userstate["status"]

        if proceed:
            try:
                task = imapsync.delay(
                    ldapuri=self.ldapuri
                    ,plevel=self.plevel
                    ,dryrun=self.dryrun
                    ,runlimit=self.runlimit
                    ,state_memcaches=self.state_memcaches
                    ,nosync_memcaches=self.nosync_memcaches
                    ,imapserver=self.imapserver
                    ,adminuser=self.adminuser
                    ,user=user
                )

            except: # Problem launching the process? Return False.
                return {"submitted":False,"reason":"task submission error"}

            cachedata = {"status":"queued", "timestamp":int(time()), "taskid":task.task_id}

            if cache.cas(cachekey, cachedata, time=3600) == True:   # Return the task_id
                return {"submitted":True,"taskid":task.task_id}

            else: # We had some trouble with the cache. Revoke the process and return None.
                task.revoke()   # If this throws an exception, we have problems.
                return {"submitted":False,"reason":"cache cas error"}

        else:
            return {"submitted":False,"reason":reason}


    def launchgroup(self, interval=0.5):
        """Launches the tasksets created using populate(). Interval is the time between submissions."""
        for userlist in self.userlists:
            for user in userlist:
                launchstatus = self.launchuser(user=user)
                if launchstatus["submitted"] == True:
                    print("user %s : task id %s" % (user, launchstatus["taskid"]))
                    sleep(interval)
                elif launchstatus["reason"] == "ok" or launchstatus["reason"] == "error_255":
                    print("user %s : %s" % (user, launchstatus["reason"]))
                    userlist.remove(user)
