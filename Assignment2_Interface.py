
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cursor=openconnection.cursor()
    cursor.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    x=cursor.fetchone()
    f = open(outputPath, "w")
    f.close()
    f = open(outputPath, "a")
    #print(x)
    
    for i in range (x[0]):
        cursor.execute("select * from RangeRatingsPart"+str(i)+" where rating>="+str(ratingMinValue)+"and rating<="+str(ratingMaxValue))
        #print(i)
        for line in cursor:
            #print("RangeRatingsPart"+str(i)+","+str(line[0])+","+str(line[1])+","+str(line[2]))
            f.write("RangeRatingsPart"+str(i)+","+str(line[0])+","+str(line[1])+","+str(line[2]))
            f.write("\n")
            
    #roundrobin        
    cursor.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    x=cursor.fetchone()
    #print(x)
    for i in range (x[0]):
        cursor.execute("select * from RoundRobinRatingsPart"+str(i)+" where rating>="+str(ratingMinValue)+"and rating<="+str(ratingMaxValue))
        #print(i)
        for line in cursor:
            #print(line)
            f.write("RoundRobinRatingsPart"+str(i)+","+str(line[0])+","+str(line[1])+","+str(line[2]))
            f.write("\n")

def PointQuery(ratingValue, openconnection,outputPath):
    cursor=openconnection.cursor()
    cursor.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    x=cursor.fetchone()
    f = open(outputPath, "w")
    f.close()
    f = open(outputPath, "a")
    #range
    for i in range (x[0]):
        cursor.execute("select * from RangeRatingsPart"+str(i)+" where rating="+str(ratingValue))
        #print(i)
        for line in cursor:
            #print(line)
            f.write("RangeRatingsPart"+str(i)+","+str(line[0])+","+str(line[1])+","+str(line[2]))
            f.write("\n")
    
    #round robin
    cursor.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    x=cursor.fetchone()
    #print(x)
    for i in range (x[0]):
        cursor.execute("select * from RangeRatingsPart"+str(i)+" where rating="+str(ratingValue))
        #print(i)
        for line in cursor:
            #print(line)
            f.write("RoundRobinRatingsPart"+str(i)+","+str(line[0])+","+str(line[1])+","+str(line[2]))
            f.write("\n")
    
