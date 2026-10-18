"""Сверка рабочих объектов PostgreSQL и MongoDB с чистой инициализацией проекта."""
import importlib.util, json, os
from uuid import uuid4
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg import sql
from pymongo import MongoClient
import dwh_lib as etl

spec=importlib.util.spec_from_file_location('lab_setup','/opt/airflow/scripts/setup.py')
setup=importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)
queries={
 'schemas': "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname<>'information_schema' ORDER BY 1",
 'columns': """SELECT n.nspname,c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
 a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a
 JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND a.attnum>0 AND NOT a.attisdropped
 AND c.relkind IN ('r','p','v','m','f') ORDER BY 1,2,a.attnum""",
 'constraints': """SELECT n.nspname,c.relname,k.conname,k.contype,pg_get_constraintdef(k.oid)
 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' ORDER BY 1,2,3""",
 'indexes': "SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes WHERE schemaname NOT LIKE 'pg_%' AND schemaname<>'information_schema' ORDER BY 1,2,3",
 'triggers': """SELECT n.nspname,c.relname,t.tgname,pg_get_triggerdef(t.oid),t.tgenabled FROM pg_trigger t
 JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE NOT t.tgisinternal AND n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' ORDER BY 1,2,3""",
 'functions': """SELECT n.nspname,p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid)
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' ORDER BY 1,2,3""",
 'objects': """SELECT i.type,i.schema,i.name,i.identity FROM pg_depend d
 JOIN pg_namespace n ON d.refobjid=n.oid CROSS JOIN LATERAL pg_identify_object(d.classid,d.objid,d.objsubid) i
 WHERE d.refclassid='pg_namespace'::regclass AND n.nspname NOT LIKE 'pg_%'
 AND n.nspname<>'information_schema' ORDER BY 1,2,3,4""",
 'foreign': """SELECT n.nspname,c.relname,s.srvname,f.ftoptions FROM pg_foreign_table f
 JOIN pg_class c ON c.oid=f.ftrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_foreign_server s ON s.oid=f.ftserver ORDER BY 1,2""",
 'extensions': """SELECT e.extname,e.extversion,n.nspname FROM pg_extension e
 JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY 1""",
 'sequences': """SELECT schemaname,sequencename,data_type,start_value,min_value,max_value,increment_by,cycle,cache_size
 FROM pg_sequences WHERE schemaname NOT LIKE 'pg_%' ORDER BY 1,2""",
 'servers': """SELECT s.srvname,w.fdwname,s.srvtype,s.srvversion,s.srvoptions
 FROM pg_foreign_server s JOIN pg_foreign_data_wrapper w ON w.oid=s.srvfdw ORDER BY 1""",
 'wrappers': """SELECT fdwname,fdwhandler::regproc::text,fdwvalidator::regproc::text,fdwoptions
 FROM pg_foreign_data_wrapper ORDER BY 1"""
}
def inventory(name):
    """Снимает структуру без данных и меняющихся идентификаторов объектов."""
    with etl.connect(name) as conn:
        conn.execute('SET LOCAL search_path=pg_catalog')
        return {key:conn.execute(query).fetchall() for key,query in queries.items()}

def mongo_inventory(name):
    """Снимает состав коллекций и индексов, исключая содержимое документов."""
    with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
        db=client[name]
        result={}
        for info in db.list_collections():
            indexes=[]
            for index in db[info['name']].list_indexes():
                definition={k:v for k,v in index.items() if k not in ('v','ns','key')}
                definition['key']=list(index['key'].items())
                indexes.append(definition)
            result[info['name']]={'type':info['type'],'options':info.get('options',{}),
                                  'indexes':sorted(indexes,key=lambda i:i['name'])}
        return result


def check_consistency():
    """Создаёт временные базы, сравнивает структуру и удаляет временные объекты в конце."""
    original={key:os.environ.get(key) for key in ('SOURCE_PG_DSN','DWH_DSN','SOURCE_MONGO_DB')}
    live={key:inventory(key) for key in ('SOURCE_PG_DSN','DWH_DSN')}
    live_mongo=mongo_inventory(original['SOURCE_MONGO_DB'] or 'restaurant_source')
    name='lab_audit_'+uuid4().hex
    admins=[]
    try:
        for key in ('SOURCE_PG_DSN','DWH_DSN'):
            conn=psycopg.connect(original[key],autocommit=True)
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            admins.append(conn)
            options=conninfo_to_dict(original[key])
            options['dbname']=name
            os.environ[key]=make_conninfo(**options)
        os.environ['SOURCE_MONGO_DB']=name
        setup.initialize_databases()
        diffs={}
        for key in ('SOURCE_PG_DSN','DWH_DSN'):
            fresh=inventory(key)
            for category in queries:
                if live[key][category]!=fresh[category]:
                    live_rows={json.dumps(row,ensure_ascii=False,default=str) for row in live[key][category]}
                    fresh_rows={json.dumps(row,ensure_ascii=False,default=str) for row in fresh[category]}
                    diffs[key+'.'+category]={'only_live':sorted(live_rows-fresh_rows),'only_fresh':sorted(fresh_rows-live_rows)}
        fresh_mongo=mongo_inventory(name)
        if live_mongo!=fresh_mongo:
            diffs['MongoDB']={'working':live_mongo,'fresh':fresh_mongo}
        print(json.dumps({'расхождения':diffs,'разделов_проверки_PostgreSQL':len(queries),
                          'коллекций_MongoDB':len(fresh_mongo)},ensure_ascii=False,indent=2))
        return not diffs
    finally:
        with MongoClient(os.environ['SOURCE_MONGO_URI']) as client:
            client.drop_database(name)
        for conn in admins:
            conn.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
            conn.close()
        for key,value in original.items():
            if value is None:
                os.environ.pop(key,None)
            else:
                os.environ[key]=value


if __name__ == '__main__':
    raise SystemExit(0 if check_consistency() else 1)

