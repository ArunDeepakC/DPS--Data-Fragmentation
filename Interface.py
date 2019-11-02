#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
	cr = openconnection.cursor()
	cr.execute("create table "+ratingstablename+" (UserID int, buf1 varchar(10),  MovieID int , buf2 varchar(10),  Rating float, buf3 varchar(10), Timestamp int)")
	dat_file= open(ratingsfilepath,'r')
	cr.copy_from(dat_file,ratingstablename,sep = ':',columns=('UserID','buf1','MovieID','buf2','Rating','buf3','Timestamp'))
	cr.execute("alter table "+ratingstablename+" drop column buf1, drop column buf2,drop column buf3, drop column Timestamp")
	cr.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):

	cr=openconnection.cursor()
	ran=float(5.0/numberofpartitions)
	init=0
	flag=0
	
	cr.execute("create table meta_range(partnum int);")
	cr.execute("insert into meta_range(partnum) values(%s);",str(numberofpartitions))
	for i in range(numberofpartitions):
		if flag==0:
			cr.execute("create table range_part"+str(i)+" as select * from "+ratingstablename+" where rating>="+str(init)+" AND rating<="+str(init+ran)+";")
			init+=ran
			flag=1
		else:
			cr.execute("create table range_part"+str(i)+" as select * from "+ratingstablename+" where rating>"+str(init)+" AND rating<="+str(init+ran)+";")
			init+=ran
	cr.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
	cr=openconnection.cursor()
	for i in range(numberofpartitions):
		cr.execute("create table rrobin_part"+str(i)+" (Userid int,Movieid int,Rating float);")
	cr.execute("select Userid,Movieid,Rating from "+ratingstablename+";")
	tab=cr.fetchall()
	count=0
	for record in tab:
		cr.execute("insert into rrobin_part"+ str(count%numberofpartitions)+"(Userid,Movieid,Rating) values"+str(record)+";")
		count=count+1
	cr.execute("create table meta_rrobin(partnum int,count int);")
	cr.execute("insert into meta_rrobin(partnum,count) values(%s,%s);",(str(numberofpartitions),str(count)))
	
	cr.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
 	cr=openconnection.cursor()
	cr.execute("insert into "+ratingstablename+" (UserID,MovieID,Rating) values (%s, %s, %s)",(userid, itemid, rating))
	cr.execute("select * from meta_rrobin;")
	part_rr,count=cr.fetchone() 
	cr.execute("insert into rrobin_part"+str(count%part_rr)+" values(%s,%s,%s);",(str(userid),str(itemid),str(rating)))
	count=count+1
	cr.execute("update meta_rrobin set count="+str(count)+";")
	cr.close()
	
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	cr=openconnection.cursor()
	cr.execute("select * from meta_range;")
	num_part=cr.fetchone()[0]
	ran=float(5.0/num_part)
	l= 0
	partitionnumber = 0
	u= ran

	while l<5.0:
		if l == 0:
		    if rating >= l and rating <= u:
			break
		    partitionnumber = partitionnumber + 1
		    l = l + ran
		    u = u + ran
		else: 
		    if rating > l and rating <= u:
			break
		    partitionnumber = partitionnumber + 1
		    l = l + ran
		    u = u + ran  
	    
	cr.execute("insert into "+ratingstablename+" (UserID,MovieID,Rating) values (%s, %s, %s)",(userid, itemid, rating))
	cr.execute("insert into range_part"+str(partitionnumber)+" (UserID,MovieID,Rating) values (%s, %s, %s)",(userid, itemid, rating))
	cr.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
