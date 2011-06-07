#!/usr/bin/python

import ldap, getpass

class psuldap:
    def __init__(self, cacertdir="/etc/pki/CA/certs"):
        """Initializes psuldap object, which is a wrapper around the standard ldap library--<cacertdir> is the directory where the CA certs can be found."""
        ldap.set_option(ldap.OPT_X_TLS_CACERTDIR, cacertdir)
    

    def connect(self, ldapurl=None, userdn=None, password=None):
        """Connects via LDAP, initalizes a TLS connection, and binds to the <ldapurl> as the user <userdn> with the supplied <password>."""
        self.conn = ldap.initialize(ldapurl)
        self.conn.start_tls_s()
        self.conn.simple_bind_s(userdn, password)

    
    def mod_attribute(self, dn, attrname, value):
        """Changes <dn>'s <attrname> to <value>."""
        self.conn.modify_s(
            dn
            ,[(ldap.MOD_REPLACE, attrname, value)]
        )


    def add_attribute(self, dn, attrname, value):
        """Adds <attrname>=<value> to a <dn>."""
        self.conn.modify_s(
            dn
            ,[(ldap.MOD_ADD, attrname, value)]
        )
            

    def del_attribute(self, dn, attrname, value):
        """Removes <attrname>=<value> from a <dn>."""
        self.conn.modify_s(
            dn
            ,[(ldap.MOD_DELETE, attrname, value)]
        )


    def search(self, searchbase="dc=pdx,dc=edu", searchfilter=None, attrlist=None):
        """Conducts a subtree search from <searchbase>, using <searchfilter>. If <attrlist> is None, then all attributes are returned. Returns a list of 2-tuples, the first element being the dn of the record, the second element being a dictionary where the keys are attribute names and items are a list of values."""
        return self.conn.search_s(
            searchbase
            ,ldap.SCOPE_SUBTREE
            ,searchfilter
            ,attrlist
        )


if __name__ == "__main__":
    ldapserver = raw_input("LDAP URL: ")
    binduser = raw_input("Binding DN: ")
    bindpw = getpass.getpass()
    query = raw_input("LDAP Search Query: ")
    ld = psuldap()
    ld.connect(ldapurl = ldapserver, userdn = binduser, password = bindpw)
    print ld.search(searchfilter = query)