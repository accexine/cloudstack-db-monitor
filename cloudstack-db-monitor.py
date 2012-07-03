# -*- coding : utf-8 -*-
# filename : 
# D:\mydoc\SkyDrive\Document\cloudstack-db-monitor


import os
import sys
import json
import datetime
from sqlalchemy         import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.dialects.sqlite import \
            BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, FLOAT, \
            INTEGER, NUMERIC, SMALLINT, TEXT, TIME, TIMESTAMP, \
            VARCHAR

class CloudStackDbMonitor:
    '''
    just monitor add/delete row changes 
    can not monitor update change.
    data is stored in sqlite .
    '''
    def __init__(self , conn_str =None ):
        pass
    def get_table( self, table_name ):
        t = Table( table_name , self.metadata , autoload= True )  
        return t 
        
        
    def create_table_list( self , meta  , engine):
        '''   
        table_list table , monitor table list.
        '''
        table_name = 'table_list'
        hockey_table=None
        schema = None
        hockey_table = Table( table_name , meta,
                                  Column('id', Integer ,   primary_key=True),
                                  Column('name', String, nullable = False ,  unique=True,  index=True),
                                  Column('process_status', Integer ,    default=0),
                                  Column('record_date', DateTime, nullable=False, default=func.now()),
                                  Column('updated', DateTime, onupdate=datetime.datetime.now),
                                  schema = schema,
                                  sqlite_autoincrement=True
                                )        
        if engine.dialect.has_table(engine.connect(), table_name ):
            print " table already exist: ",table_name
        else:
            print " table not exist , so create : " ,table_name
            schema = None
            #meta.create_all() 
            hockey_table.create(engine , checkfirst=True )#single table  
        if self.is_drop_exist_table:
            user_input = raw_input( " DEBUG MODE drop table Y or n " )
            if "Y" == user_input:
                hockey_table.drop(  engine )      
        pass

    def create_table_status( self , meta  , engine):
        '''   
        table_status table , monitor table rows change 
        in json format
        '''
        table_name = 'table_status'
        hockey_table=None
        schema = None
        hockey_table = Table( table_name , meta,
                                  Column('id', Integer ,   primary_key=True),
                                  Column('table_name', String, nullable = False ,  index=True),
                                  Column('table_status', String, nullable = False ),
                                  Column('process_status', Integer ,    default=0),
                                  Column('record_date', DateTime, nullable=False, default=func.now()),
                                  Column('updated', DateTime, onupdate=datetime.datetime.now),
                                  schema = schema,
                                  sqlite_autoincrement=True
                                )        
        if engine.dialect.has_table(engine.connect(), table_name ):
            print " table already exist: ",table_name
        else:
            print " table not exist , so create : " ,table_name
            schema = None
            #meta.create_all() 
            hockey_table.create(engine , checkfirst=True )#single table  
        if self.is_drop_exist_table:
            user_input = raw_input( " DEBUG MODE drop table Y or n " )
            if "Y" == user_input:
                hockey_table.drop(  engine )      
        pass
    
    