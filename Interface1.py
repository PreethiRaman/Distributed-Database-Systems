import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        c = openconnection.cursor()
        c.execute("DROP TABLE IF EXISTS "+ratingstablename)
        c.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
        
        x = open(ratingsfilepath,'r')
        
        c.copy_from(x,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
        c.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if c:
            c.close()

    


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        c = openconnection.cursor()
        inter = float(5 / numberofpartitions)
        st = float(0)
        pn = 0
        en = float(st + inter)
        while(pn < numberofpartitions):
            if pn == 0:
                c.execute("DROP TABLE IF EXISTS range_part" + str(pn))
                c.execute("CREATE TABLE range_part"+str(pn)+
                " AS SELECT * FROM "+ratingstablename+" WHERE rating>="+str(st)+
                " AND rating<="+str(en)+";")
                st = float(st + inter)
                en = float(st + inter)
                pn = pn + 1
                openconnection.commit()
            else:
                c.execute("DROP TABLE IF EXISTS range_part"+str(pn))
                c.execute("CREATE TABLE range_part" + str(pn) +
                        " AS SELECT * FROM " + ratingstablename + " WHERE rating>" + str(st) +
                        " AND rating<=" + str(en) + ";")
                st = float(st + inter)
                en = float(st + inter)
                pn = pn + 1
                openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
        openconnection.rollback()
    print('Error %s' % e)
    finally:
        if c:
            c.close()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        c = openconnection.cursor()
        for i in range(0, numberofpartitions):
            c.execute("DROP TABLE IF EXISTS rrobin_part"+str(i))
            c.execute(" CREATE TABLE rrobin_part" + str(i) + " (userid, movieid, rating) AS SELECT temp.userid, temp.movieid, temp.rating"
                        " FROM ( select row_number() over() as rn, userid, movieid, rating "
                        " FROM " + ratingstablename + " ) as temp "
                        " WHERE (temp.rn - 1) % "+ str(numberofpartitions) + " = "+ str(i) + " ;")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
        openconnection.rollback()
    print('Error %s' % e)
    finally:
        if c:
            c.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        c = openconnection.cursor()
        c.execute("SELECT table_name FROM information_schema.tables WHERE table_name like'%rrobin_part%'; ")
        at = c.fetchall()
        c.execute("SELECT count(*) FROM "+at[0][0])
        st = c.fetchone()[0]
        tmp = st
        for table_number in range(1, len(at)):
            c.execute("SELECT count(*) FROM "+at[table_number][0])
            ntbl = c.fetchone()[0]
            if ntbl < tmp:
                c.execute("INSERT INTO " + at[table_number][0] + " (userid, movieid, rating)values ("
                +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
                break
            tmp = ntbl
        if table_number+1==len(at) and tmp==st:
            c.execute("INSERT INTO "+at[0][0]+" (userid, movieid, rating) values ("
            +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
        c.execute("INSERT INTO "+ratingstablename+" (userid, movieid, rating) values ( " 
        +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
        openconnection.rollback()
    print('Error %s' % e)
    finally:
        if c:
            c.close()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        c = openconnection.cursor()
        c.execute("SELECT table_name FROM information_schema.tables WHERE table_name like '%range_part%'; ")
        at = c.fetchall()
        inter = float(5) / len(at)
        st = float(0)
        en = float(inter)
        table_number = 0
        while table_number<len(at):
            if table_number==0:
                if rating >= st and rating <= en:
                    c.execute("INSERT INTO "+at[table_number][0]+"(userid, movieid, rating) values ( " 
                    +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
                    break
            else:
                if rating > st and rating <= en:
                    c.execute("INSERT INTO "+at[table_number][0]+"(userid, movieid, rating) values( " 
                    +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
                    break
            st = float(st + inter)
            en = float(st + inter)
            table_number += 1
        c.execute("INSERT INTO "+ratingstablename+" (userid, movieid, rating) values ( " 
        +str(userid)+" , "+str(itemid)+" , "+str(rating)+")")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
        openconnection.rollback()
    print('Error %s' % e)
    finally:
        if c:
            c.close()

def createDB(dbname='dds_assignment1'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
