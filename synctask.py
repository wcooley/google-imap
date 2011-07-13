from celery.task import task
from os import uname
from time import sleep, time
from psuldap import psuldap
import shlex, subprocess
import memcache

@task(ignore_result=True)
def imapsync(ldapuri=None, state_memcaches=None, nosync_memcaches=None, imapserver=None, adminuser=None, plevel="test", dryrun=True, runlimit=86400, user=None):
    imapsync_dir = "/opt/google-imap/"
    imapsync_cmd = imapsync_dir + "imapsync"
    cyrus_pf = imapsync_dir + "cyrus.pf"
    exclude_list = "'^Shared Folders|^mail/|^Junk$|^junk$|^JUNK$'"
    whitespace_cleanup = " --regextrans2 's/[ ]+/ /g' --regextrans2 's/\s+$//g' --regextrans2 's/\s+(?=\/)//g' --regextrans2 's/^\s+//g' --regextrans2 's/(?=\/)\s+//g'"
    folder_cases = " --regextrans2 's/^drafts$/drafts2/' --regextrans2 's/^trash$/trash2/' --regextrans2 's/^sent$/sent2/' --regextrans2 's/^sent-mail$/sent-mail2/' --regextrans2 's/^templates$/templates2/'"
    extra_opts = " --delete2 --delete2folders"
    exitstatus = "premature"

    if dryrun:
        extra_opts = extra_opts + " --dry" 

    if runlimit <= 0:
        raise Exception("Runlimit must be a positive integer.")

    if plevel == "prod":
        google_pf = imapsync_dir + "google-prod.pf"
        google_domain = "pdx.edu"

    elif plevel == "test":
        google_pf = imapsync_dir + "google-test.pf"
        google_domain = "gtest.pdx.edu"

    else:
        raise Exception("Plevel must be test or prod.")

    command = imapsync_cmd + " --pidfile /tmp/imapsync-" + user + ".pid --host1 " + imapserver + " --port1 993 --user1 " + user + " --authuser1 " + adminuser + " --passfile1 " + cyrus_pf + " --host2 imap.gmail.com --port2 993 --user2 " + user + "@" + google_domain + " --passfile2 " + google_pf + " --ssl1 --ssl2 --maxsize 26214400 --authmech1 PLAIN --authmech2 XOAUTH -sep1 '/' --exclude " + exclude_list + folder_cases + whitespace_cleanup + extra_opts

    cache = memcache.Client(servers=state_memcaches)            # System state
    nosync_cache = memcache.Client(servers=nosync_memcaches)    # Users not-to-sync

    cachekey = "(%s,auto)" % user

    nosyncstate = nosync_cache.get(cachekey)

    if nosyncstate != None:     # If the key exists in this cache, skip the sync.
        exitstatus = "nosync"

    directory = psuldap()       # Our LDAP handle
    directory.connect(ldapuri)  # Anonymous bind

    # Search for the user's mailHost attribute.
    mailhostsearch = directory.search(searchfilter="(uid=%s)" % user, attrlist=["mailHost"]) 

    # Loop through the search results. If any them match gmx.pdx.edu, set in the nosync cache.
    for (dn, result) in mailhostsearch:
        if result.has_key("mailHost"):
            for mailhost in result["mailHost"]:
                if mailhost == "gmx.pdx.edu":
                    if nosync_cache.set(cachekey,{"status":"nosync"}) != True:
                        raise Exception("Could not set %s in nosync_cache." % cachekey)

                    exitstatus = "nosync"

    if exitstatus == "nosync":  # User has been, or is newly added to nosync list. Let's exit.
        return (user, exitstatus)

    # We can continue.
    cachestate = cache.gets(cachekey)

    if cachestate == None:  # Maybe the cache has been cleared. Continue.
        pass

    # Well, it looks like the cache has another task's data for this user. Abort.
    elif cachestate["status"] != "queued" or cachestate["taskid"] != imapsync.request.id:
        raise Exception("Cache inconsistency error for user %s." % user)

    # We're good to go. Let's set the cache with our new state.
    runstate = {
        "status":"running"
        ,"timestamp":int(time())
        ,"taskid":imapsync.request.id
        ,"worker":uname()[1]
    }

    cachelimit = runlimit + 10  # Fudge factor. 5 seconds for the sleep, 5 seconds for fudge.
    
    if cache.cas(cachekey, runstate, time=cachelimit) != True: # Whoops, something changed. Abort.
        raise Exception("Cache inconsistency error for user %s." % user)

    syncprocess = subprocess.Popen(
        args=shlex.split(command)
        ,bufsize=-1
        ,close_fds=True
        ,stdout=open("/dev/null",'w')
        ,stderr=None
    )

    starttime = time()

    # While the process is running, and we're under the time limit
    while (syncprocess.poll() == None) and ((time() - starttime) < runlimit):
        sleep(5)
    
    # Are we still running? Send a SIGTERM to the process and throw the task back on the queue.
    # This is done to prevent one user from tying up a worker for longer than the runlimit.
    if (syncprocess.poll() == None):
        syncprocess.terminate()     # Send SIGTERM
        syncprocess.communicate()   # Read stdin/out, and wait for process to terminate
        exitstatus = "outtatime"

    else:
        if syncprocess.returncode == 0:
            exitstatus = "ok"

        else:
            exitstatus = "error_%d" % syncprocess.returncode

    cachestate = cache.gets(cachekey)

    if cachestate == None: # Maybe the cache has been cleared. Continue.
        pass

    # Check to see if our cache state matches the previously set state. 
    if cachestate != runstate:
        raise Exception("Cache inconsistency error for user %s." % user)

    # We're good to go. Let's set the cache with our new state.
    endstate = {
        "status":"complete"
        ,"timestamp":int(time())
        ,"taskid":imapsync.request.id
        ,"worker":uname()[1]
        ,"returned":exitstatus
        ,"runtime":int(time() - starttime)
    }
    
    if cache.cas(cachekey, endstate) != True: # Whoops, something changed. Abort.
        raise Exception("Cache inconsistency error for user %s." % user)

    return (user, exitstatus)
