import email
import imaplib
from pyparsing import Word, alphas, nums, printables, ZeroOrMore

class imapstat:
    def __init__(self, imapserver=None, adminuser=None, adminpassword=None):
        """Sets parameters for the object: <imapserver>, <adminuser>, and <adminpassword>."""
        self.imapserver = imapserver
        self.adminuser = adminuser
        self.adminpassword = adminpassword


    def connect(self, user = None):
        """Establishes a connection for <user>."""
        authstring = "%s\x00%s\x00%s" % (user, self.adminuser, self.adminpassword)

        self.imap = imaplib.IMAP4_SSL(self.imapserver)
        self.imap.authenticate("PLAIN", lambda x: authstring)


    def disconnect(self):
        """Closes an established IMAP connection."""
        self.imap.logout()


    def mboxstat(self, mbox):
        """Sends an IMAP select against the named <mbox>, returning True if the command succeeds, False otherwise."""
        sele_ret, msgs_cnt = self.imap.select(mbox, readonly = True)

        if sele_ret == "OK":
            return True
        else: 
            return False


    def parsequota(self, rawdata):
        """Takes the raw output from a IMAP getquotaroot command, like so:
        [['INBOX INBOX'], ['INBOX (STORAGE 151788 1000000)']]

        Returns a 2-tuple, (quota_used, quota)

        Example:

        >>> ims = imapstat()
        >>> good = [['INBOX INBOX'], ['INBOX (STORAGE 151788 1000000)']]
        >>> bad = [['INBOX INBOX'], ['YUCK INBOX (STORAGE 151788 1000000)']]
        >>> ims.parsequota(rawdata = good)
        (151788, 1000000)
        >>> ims.parsequota(rawdata = bad)
        Traceback (most recent call last):
          File "/usr/lib64/python2.7/doctest.py", line 1248, in __run
            compileflags, 1) in test.globs
          File "<doctest __main__.imapstat.parsequota[4]>", line 1, in <module>
            ims.parsequota(rawdata = bad)
          File "./imapstat.py", line 70, in parsequota
            raise Exception("Error parsing: %s" % rawdata[1])
        Exception: Error parsing: ['YUCK INBOX (STORAGE 151788 1000000)']
        """
        rootname = Word(alphas)
        resource = Word(alphas)
        quota_used = Word(nums)
        quota = Word(nums)

        quota_form = rootname + '(' + resource + quota_used + quota + ')'

        quota_parse = quota_form.parseString

        quota, quota_used = (0, 0)

        try:
            parsed = quota_parse(rawdata[1][0])
            quota_used = int(parsed[3])
            quota = int(parsed[4])

        except:
            raise Exception("Error parsing: %s" % rawdata[1])

        return (quota_used, quota)


    def parsemboxlist(self, rawdata):
        """Takes the raw output from a IMAP list mailboxes command, like so:
        ['(\\Noinferiors) "/" "INBOX"', '(\\HasNoChildren) "/" "Drafts"']
        Notice how it's a list of quoted strings.

        Returns a list of quoted mailbox names "INBOX", "Drafts"...

        Example:

        >>> ims = imapstat()
        >>> good = ['(\\Noinferiors) "/" "INBOX"', '(\\HasNoChildren) "/" "Drafts"', '(\\HasNoChildren) "/" "I Love Spam"', '(\\HasNoChildren) "/" "Notes"', '(\\HasNoChildren) "/" "Sent"', '(\\HasChildren) "/" "Trash"', '(\\HasNoChildren) "/" "Trash/Sent"', '(\\HasNoChildren) "/" "Trash/Sent Messages"', '(\\HasChildren) "/" "_CECS"', '(\\HasNoChildren) "/" "_CECS/Announce"', '(\\HasNoChildren) "/" "_CECS/Asynchronous"', '(\\HasNoChildren) "/" "_CECS/CAT"', '(\\HasNoChildren) "/" "_CECS/CS162"', '(\\HasNoChildren) "/" "_CECS/CS163"', '(\\HasNoChildren) "/" "_CECS/CS201"', '(\\HasNoChildren) "/" "_CECS/CS202"']
        >>> bad = ['(\\Noinferiors) "/\\\\\\\\\\\\\\\\\\" "INBOX"']
        >>> ims.parsemboxlist(good)
        ['"_CECS/Asynchronous"', '"_CECS/CS162"', '"Trash/Sent Messages"', '"_CECS/CAT"', '"_CECS/Announce"', '"_CECS"', '"_CECS/CS201"', '"I Love Spam"', '"Sent"', '"INBOX"', '"Trash/Sent"', '"Notes"', '"Drafts"', '"Trash"', '"_CECS/CS163"', '"_CECS/CS202"']
        >>> ims.parsemboxlist(bad)
        Traceback (most recent call last):
          File "<input>", line 1, in <module>
          File "imapstat.py", line 98, in parsemboxlist
            raise Exception("Error parsing %s" % raw_mbox)
        Exception: Error parsing (\Noinferiors) "/\\\\\\\\\" "INBOX"
        """
        flags = Word(alphas + '\\')
        root = Word(alphas + '/')
        mboxname = Word(printables + ' ')

        mbox_form = '(' + ZeroOrMore(flags) + ')' + '"' + root  + '"' + mboxname

        mbox_parse = mbox_form.parseString

        parsed = set()

        for raw_mbox in rawdata:
            try:
                parsed.add(mbox_parse(raw_mbox).pop())

            except:
                raise Exception("Error parsing %s" % raw_mbox)

        return list(parsed)


    def parseheader(self, header_list):
        """Takes the raw output from an IMAP fetch of message headers in <header_list>, and Returns a list of dicts, each dict corresponding to a separate message header.

        Example:

        >>> ims = imapstat()
        >>> good = [('121 (RFC822.HEADER {1563}', 'Return-Path: <user@dom.tld>\\r\\nReceived: from murder (server.sub.dom.tld [192.168.122.1])\\r\\n\\t by server05.mail.dom.tld (Cyrus v2.3.7-Invoca-RPM-2.3.7-7.el5_4.3) with LMTPSA\\r\\n\\t (version=TLSv1/SSLv3 cipher=AES256-SHA bits=256/256 verify=YES);\\r\\n\\t Thu, 26 May 2011 12:20:56 -0700\\r\\nX-Sieve: CMU Sieve 2.3\\r\\nReceived: from server.sub.dom.tld ([unix socket])\\r\\n\\t by mail.dom.tld (Cyrus v2.2.13) with LMTPA;\\r\\n\\t Thu, 26 May 2011 12:20:56 -0700\\r\\nReceived: from server.sub.dom.tld (server.sub.dom.tld [192.168.111.42])\\r\\n\\tby server.sub.dom.tld (8.14.1+/8.13.1) with ESMTP id p4QJKup5003028\\r\\n\\tfor <user@server.dom.tld>; Thu, 26 May 2011 12:20:56 -0700\\r\\nReceived: from server-06.sub.dom.tld (server-06.sub.dom.tld [192.168.120.172])\\r\\n\\t(authenticated bits=0)\\r\\n\\tby server.sub.dom.tld (8.13.8/8.13.1) with ESMTP id p4QJKtW8021832\\r\\n\\t(version=TLSv1/SSLv3 cipher=DHE-RSA-AES256-SHA bits=256 verify=NOT)\\r\\n\\tfor <user@dom.tld>; Thu, 26 May 2011 12:20:55 -0700\\r\\nReceived: from server.sub.dom.tld (server.sub.dom.tld\\r\\n [192.168.132.34]) by server.dom.tld (Horde Framework) with HTTP; Thu, 26\\r\\n May 2011 12:20:55 -0700\\r\\nMessage-ID: <20110526122055.97746emge2ucfk7r@server.dom.tld>\\r\\nDate: Thu, 26 May 2011 12:20:55 -0700\\r\\nFrom: user@dom.tld\\r\\nTo: user@dom.tld\\r\\nSubject: Test Email\\r\\nMIME-Version: 1.0\\r\\nContent-Type: text/plain;\\r\\n charset=ISO-8859-1;\\r\\n DelSp="Yes";\\r\\n format="flowed"\\r\\nContent-Disposition: inline\\r\\nContent-Transfer-Encoding: 7bit\\r\\nUser-Agent: Dynamic Internet Messaging Program (DIMP) H3 (1.1.4)\\r\\nX-Scanned-By: MIMEDefang 2.71 on 192.168.111.42\\r\\n\\r\\n'), ')']
        >>> ims.parseheader(good)
        [{'Received': 'from murder (server.sub.dom.tld [192.168.122.1])\\r\\n\\t by server05.mail.dom.tld (Cyrus v2.3.7-Invoca-RPM-2.3.7-7.el5_4.3) with LMTPSA\\r\\n\\t (version=TLSv1/SSLv3 cipher=AES256-SHA bits=256/256 verify=YES);\\r\\n\\t Thu, 26 May 2011 12:20:56 -0700', 'X-Sieve': 'CMU Sieve 2.3', 'From': 'user@dom.tld', 'Return-Path': '<user@dom.tld>', 'MIME-Version': '1.0', 'Content-Transfer-Encoding': '7bit', 'X-Scanned-By': 'MIMEDefang 2.71 on 192.168.111.42', 'User-Agent': 'Dynamic Internet Messaging Program (DIMP) H3 (1.1.4)', 'To': 'user@dom.tld', 'Date': 'Thu, 26 May 2011 12:20:55 -0700', 'Message-ID': '<20110526122055.97746emge2ucfk7r@server.dom.tld>', 'Content-Type': 'text/plain;\\r\\n charset=ISO-8859-1;\\r\\n DelSp="Yes";\\r\\n format="flowed"', 'Content-Disposition': 'inline', 'Subject': 'Test Email'}]
        """
        return [
            dict(email.message_from_string(x[1]))
            for x in header_list if x != ')'
        ]


    def quotastat(self):
        """Returns the current user's IMAP quota as the tuple: (quota_used, quota)."""
        quot_ret, quota_raw = self.imap.getquotaroot("INBOX")

        if quot_ret == "OK":
            return self.parsequota(quota_raw)
        else:
            raise Exception("Server returned invalid quota data")


    def mboxlist(self):
        """Returns a verified (can we IMAP select it?) list of a user's mailboxes."""
        mbox_ret, mbox_raw = self.imap.list()
        subm_ret, subm_raw = self.imap.lsub()

        list_raw = (mbox_raw + subm_raw)

        if (mbox_ret, subm_ret) == ("OK", "OK"):
            mbox_list = self.parsemboxlist(list_raw)

        else:
            raise Exception("Server returned invalid response to list command")

        return [x for x in mbox_list if self.mboxstat(x)]


    def bigmessages(self, user, mbox_list, lower_bound):
        "For a given <user>, and a <mbox_list> of that user's mailboxes, searches each mailbox for messages that exceed <lower_bound> bytes in size, and returns the headers of those messages in a dict of lists, where each key resolves to a list of dicts, each of which contains a separate message header."""
        msg_list = dict()

        self.connect(user)
        
        for mbox in mbox_list:
            try:
                sele_ret, msgs_cnt = self.imap.select(mbox, readonly = True)
                srch_ret, indx_raw = self.imap.search(None, "(LARGER %d)" % lower_bound)
    
                msg_headers = list()
                msg_indexes = indx_raw[0].split()
                        
                if len(msg_indexes) > 0:
                    msg_ret, msg_headers = self.imap.fetch(
                        ",".join(msg_indexes)
                        ,"RFC822.HEADER"
                    )
                
                    if msg_ret == "OK":
                        msg_list[mbox] = self.parseheader(msg_headers)

            except:
                print "Error processing mailbox %s" % mbox

        self.disconnect()

        return msg_list


    def stat(self, user):
        """For a given <user>, returns a dict containing a list of that user's accessible mailboxes, that user's quota, and how much of that quota is currently used."""
        mbox_list = list()
        quota = 0
        quota_used = 0

        self.connect(user)
        quota_used, quota = self.quotastat()
        mbox_list = self.mboxlist()
        self.disconnect()

        return {"mbox_list":mbox_list,"quota":quota,"quota_used":quota_used}


if __name__ == "__main__":
    import doctest
    doctest.testmod()
