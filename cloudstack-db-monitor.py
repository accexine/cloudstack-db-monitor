# -*- coding : utf-8 -*-
# filename : 


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

class CloudStackDb:
    def __init__( self , conn_str ):
        if conn_str is None:
            print "cloudstatic mysql initialize error with no conn str "
            sys.exit( 0 );   
        self.conn_str = conn_str
        self.engine = create_engine( self.conn_str,strategy='threadlocal', encoding='utf8', convert_unicode=True, echo = True) 
        self.engine.echo = False
        self.metadata = MetaData( self.engine )
        self.metadata.bind = self.engine
        self.conn = self.engine.connect()
        # configure Session class with desired options
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()      
            
class CloudStackDbMonitor:
    '''
    just monitor add/delete row changes 
    can not monitor update change.
    data is stored in sqlite .
    '''
    def __init__(self , sqlite_conn_str =None , cs_conn_str = None ):
        if cs_conn_str is None:
            print "cloudstack mysql initialize error with no conn str "
            sys.exit(1)
        if sqlite_conn_str is None:
            print "sqlite initialize error with no conn str "
            sys.exit(1);
        self.cs_db = CloudStackDb( cs_conn_str )
        self.conn_str = sqlite_conn_str
        self.engine = None
        e=create_engine( self.conn_str,strategy='threadlocal', encoding='utf8', convert_unicode=True, echo = True) 
        try:
            os.stat(e.url.database)
        except :#OSError
            print " database not exist  , create it first"
            cmd = raw_input( "create database or exit  ?  Y or n " )
            if cmd == "Y":
                e.echo = True
                e.connect()
                meta_data = MetaData( e )
                self.create( meta_data   , e )
                e.connect() # again
            else:
                sys.exit( 0 )
        self.engine = e
        self.engine.echo = False
        self.metadata = MetaData( self.engine )
        self.metadata.bind = self.engine
        self.conn = self.engine.connect()
        # configure Session class with desired options
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        # only for debug mode
        self.is_drop_exist_table = False       

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
    def init_tables( self ):
        tlist = self.cs_db.metadata.tables.keys()
        for t in tlist:
            print t
        pass
    
if __name__ == "__main__":
    conn_str  = "sqlite:///cloudstack.sqlite"
    cs_conn_str = ""
    csdbm = CloudStackDbMonitor( conn_str )
    csdbm.init_tables()
    
    