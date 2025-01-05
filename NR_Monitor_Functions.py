import pandas as pd

import os

import pysftp



cnopts = pysftp.CnOpts()
cnopts.hostkeys = None


# remote server credentials
host = "sftp.ext.net.nokia.com"
username = "nporeps"
password = "Nokia2023#"
port = 22

sftp_conn=pysftp.Connection(host, username=username, password=password, cnopts=cnopts)






def NR_get_clusters(s3, s3_bucket):


    s3.download_file(s3_bucket, 'Reports/NR/Scheduler/scheduled_tasks.csv', 'scheduled_tasks.csv')
    #s3.download_file(s3_bucket, 'Reports/NR/NR_Monitor/nr_black_list.csv', 'nr_black_list.csv')


    with sftp_conn.cd('ON_AIR_5G'):

        sftp_conn.get('nr_black_list.csv')


    return True



def on_air_files():

    with sftp_conn.cd('ON_AIR_5G'):

        sftp_conn.get('Alarmas ID fault Supp info.xlsx')
        sftp_conn.get('onair_seguimiento_5g.xlsx')

    return True







def generate_alarm_report(conn_PM):


    import random


    rand_str=str(random.randrange(1000, 50000, 1))

# Alarmas de Ofensores de Disponibilidad


    data_hist=pd.read_csv('NR_Monitor_KPI_Site_Day.csv')[['Date','Site','NR_5150a','cluster']]

    data_hist.sort_values(by='Date', ascending=False, inplace=True)



    


    avail_last_hour=data_hist.groupby('Site').head(1)[['Site','NR_5150a']]
    avail_last_hour.columns=['Sitio','NR_5150a']


    #data_hist.to_csv('Avail_Offenders_History.csv', index=False)


    sites_avail=list(set(data_hist['Site']))




    str_sql=f"""select distinct site_name as Sitio, moDistName
                    from NR_Site_Database t
                    inner join (
                    select sector_name, max(export_date) as MaxDate
                    from NR_Site_Database 
                    group by sector_name
                    ) tm on t.sector_name = tm.sector_name and t.export_date = tm.MaxDate 

                    
    """


    #Ejecución del QUERY 
    bl = pd.read_sql(str_sql, conn_PM).dropna()


    bl['moDistName_site']=bl['moDistName'].apply(lambda x:x[:x.find('/NRCELL')])

    bl_sites=bl[['Sitio','moDistName_site']].drop_duplicates()

    sitios_offenders=bl_sites[bl_sites['Sitio'].isin(sites_avail)]


    fx_str=f"""
    
    select DN, ALARM_TIME, CANCEL_TIME, ALARM_NUMBER, TEXT, SUPPLEMENTARY_INFO
    from default.fx_alarm t
    where DN like '%%MRBTS%%' 
    and ALARM_TIME>=today()-7
    and (
    """ + "DN like '" + "%%' or DN like '".join(sitios_offenders['moDistName_site'])+"%%')"



    fx_alarms = pd.read_sql(fx_str, conn_PM)

    fx_alarms['DN_site']=fx_alarms['DN'].apply(lambda x:x[:x.find('/NRBTS')])

    bl['DN_site']=bl['moDistName'].apply(lambda x:x[:x.find('/NRBTS')])

    off_alarms=bl[['Sitio','DN_site']].drop_duplicates().merge(fx_alarms, on='DN_site').drop('DN_site', axis=1)

    off_alarms.to_csv(f'Offenders_Alarms_{rand_str}.csv', index=False)

    print("Alarmas de ofensores generadas")





# Alarmas ANCHOR sites de ofensores



    anchor_sites=pd.read_csv('anchor_sites.csv')

    sitios_anchor=anchor_sites[['name','LNBTS_Name','mo_distname_parent']].copy()

    fx_str=f"""
    
    select DN, ALARM_TIME, CANCEL_TIME, ALARM_NUMBER, TEXT, SUPPLEMENTARY_INFO
    from default.fx_alarm t
    where DN like '%%MRBTS%%'  and CANCEL_TIME is null
    and ALARM_TIME>=today()-7
    and (
    """ + "DN like '" + "%%' or DN like '".join(sitios_anchor['mo_distname_parent'])+"%%')"



    fx_alarms = pd.read_sql(fx_str, conn_PM)

    fx_alarms['DN_site']=fx_alarms['DN'].apply(lambda x:x[:x.find('/LNBTS')])

    sitios_anchor['DN_site']=sitios_anchor['mo_distname_parent'].apply(lambda x:x[:x.find('/LNBTS')])

    anchor_alarms=sitios_anchor.drop_duplicates().merge(fx_alarms, on='DN_site').drop('DN_site', axis=1).drop('mo_distname_parent', axis=1)

    anchor_alarms.rename(columns={'name':'NRBTS_Name'}, inplace=True)

    anchor_alarms.to_csv(f'Anchor_Alarms_{rand_str}.csv', index=False)


    print("Alarmas de ANCHOR generadas")


    # Descargar diccionario y seguimiento onair

    status=on_air_files()


    print('Descarga de diccioonario exitosoa')



    alarm_dict = pd.read_excel(f'Alarmas ID fault Supp info.xlsx', engine='openpyxl')
    on_air_5g = pd.read_excel(f'onair_seguimiento_5g.xlsx', engine='openpyxl')

    on_air_5g=on_air_5g[['Site Name',
    'Region',
    'Integracion ACK',
    'GAP IMP',
    'Ejecuta',
    'Fase',
    'Tipo Pendiente',
    'Owner',
    'Ing NPO',
    'Estado Estabilidad',
    'Comentario NI',
    'FC Visita 5G',
    'FC Integracion',
    'Revision Calidad',
    'Periodo Estabilidad OK']].copy()


    alarm_dict_4g=alarm_dict[(alarm_dict['Technology']=='4G')| (alarm_dict['Technology']=='SRAN')][['Alert number (Alarm-Fault)','Alarm Name','Default Severity','Meaning']]
    alarm_dict_4g['Alert number']=alarm_dict_4g['Alert number (Alarm-Fault)'].apply(lambda x: x.split('-')[0])

    alarm_dict_4g['Alert number']=alarm_dict_4g['Alert number'].astype(int)
    alarm_dict_4g.drop('Alert number (Alarm-Fault)', axis=1, inplace=True)
    alarm_dict_4g.drop_duplicates(inplace=True)





    alarm_dict_5g=alarm_dict[(alarm_dict['Technology']=='5G')][['Alert number (Alarm-Fault)','Alarm Name','Default Severity','Meaning']]
    alarm_dict_5g['Alert number']=alarm_dict_5g['Alert number (Alarm-Fault)'].apply(lambda x: x.split('-')[0])

    alarm_dict_5g['Alert number']=alarm_dict_5g['Alert number'].astype(int)
    alarm_dict_5g.drop('Alert number (Alarm-Fault)', axis=1, inplace=True)
    alarm_dict_5g.drop_duplicates(inplace=True)


    off_alarms=pd.read_csv(f'Offenders_Alarms_{rand_str}.csv').merge(avail_last_hour, on='Sitio')

    #off_alarms= off_alarms[off_alarms['CANCEL_TIME'].isnull()]


    anchor_alarms=pd.read_csv(f'Anchor_Alarms_{rand_str}.csv')
    anchor_alarms['ALARM_NUMBER']=anchor_alarms['ALARM_NUMBER'].astype(int)

    alarms_5g_4g=off_alarms.merge(alarm_dict_5g, left_on='ALARM_NUMBER', right_on='Alert number', how='left').drop('Alert number', axis=1)\
        .merge(anchor_alarms[['NRBTS_Name','LNBTS_Name', 'DN','SUPPLEMENTARY_INFO','ALARM_NUMBER','TEXT']], left_on='Sitio', right_on='NRBTS_Name', how='left', suffixes=['_5G','_4G'])\
        .merge(on_air_5g[['Site Name','Owner']], left_on='LNBTS_Name', right_on='Site Name', how='left')\
            .merge(alarm_dict_4g, left_on='ALARM_NUMBER_4G', right_on='Alert number', how='left', suffixes=['_5G','_4G'])\
                .drop(['Site Name','Alert number'], axis=1)




    alarms_5g_4g['ALARM_NUMBER_4G']=alarms_5g_4g['ALARM_NUMBER_4G'].astype('Int64')

    alarms_5g_4g[['CANCEL_TIME','SUPPLEMENTARY_INFO_5G']] = alarms_5g_4g[['CANCEL_TIME','SUPPLEMENTARY_INFO_5G']].fillna(value='N/A')




    alarms_final=on_air_5g.merge(alarms_5g_4g.groupby(['NR_5150a','Sitio', 'DN_5G', 'ALARM_TIME', 'CANCEL_TIME', 'ALARM_NUMBER_5G','TEXT_5G', 'SUPPLEMENTARY_INFO_5G'])\
        .agg({'LNBTS_Name':lambda x: list(x),'DN_4G':lambda x: list(x),'SUPPLEMENTARY_INFO_4G':lambda x: list(x),'ALARM_NUMBER_4G':lambda x: list(x),
        'TEXT_4G':lambda x: list(x),'Owner':lambda x: list(x),'Alarm Name_4G':lambda x: list(x),'Default Severity_4G':lambda x: list(x),'Meaning_4G':lambda x: list(x)}).reset_index(),
    left_on='Site Name', right_on='Sitio', how='right', suffixes=['_5G','_4G']).dropna(subset=['Site Name'])


    #print(alarms_final.head(10))

    alarms_final=alarms_final[alarms_final['NR_5150a']<99]

    alarms_final.to_excel(f'Escalamientos_5G_{rand_str}.xlsx', engine='xlsxwriter') 

    os.remove(f'Anchor_Alarms_{rand_str}.csv')
    os.remove(f'Offenders_Alarms_{rand_str}.csv')



    return rand_str





def generate_anchor_sites():

    import prestodb


    conn = prestodb.dbapi.connect(
        host=os.getenv('presto_host'),
        port=os.getenv('presto_port'),
        user='admin',
        catalog='hive',
        schema='network_data'
    )     


    cursor = conn.cursor()

    cursor.execute(f"""

        select distinct
        LNBTS.mo_distname as mo_distname_parent,
        LNADJGNB.adjGnbId, NRBTS.name, MRBTS_Name, LNBTS_Name, LNBTS.MRBTS_id, LNBTS.LNBTS_id, NRDCDPR.NRDCDPR_id,
        ENDCDMEASCONF.carrierFreqNrCell, ENDCDMEASCONF.quantityConfigId, ENDCDMEASCONF.ssbDuration, ENDCDMEASCONF.ssbOffset, 
        ENDCDMEASCONF.ssbPeriodicity, ENDCDMEASCONF.ssbSubcarrierSpacing 

        from

        ((select mo_distname, raml_date, 
            element_at(parameters, 'name') as LNBTS_Name,
            element_at(ids, 'MRBTS') as MRBTS_id,
            element_at(ids, 'LNBTS') as LNBTS_id
            from hive.network_data.cm
            where mo_class = 'LNBTS') LNBTS inner join 
            
        (select max(raml_date) as last_date  from hive.network_data.cm where mo_class = 'LNCEL') max_date on LNBTS.raml_date=max_date.last_date inner join


        (select mo_distname, raml_date, 
            element_at(parameters, 'name') as MRBTS_Name,
            element_at(ids, 'MRBTS') as MRBTS_id,
            element_at(ids, 'LNBTS') as LNBTS_id
            from hive.network_data.cm
            where mo_class = 'MRBTS') MRBTS on MRBTS.mrbts_Id = LNBTS.mrbts_Id and MRBTS.raml_date=LNBTS.raml_date) inner join 
            

        (select mo_distname, raml_date, 
            element_at(ids, 'MRBTS') as MRBTS_id,
            element_at(ids, 'LNBTS') as LNBTS_id,
            element_at(ids, 'NRDCDPR') as NRDCDPR_id
            from hive.network_data.cm
            where mo_class = 'NRDCDPR') NRDCDPR on (LNBTS.MRBTS_id = NRDCDPR.MRBTS_id AND LNBTS.LNBTS_id = NRDCDPR.LNBTS_Id and LNBTS.raml_date=NRDCDPR.raml_date) inner join
            

        (select mo_distname, raml_date, 
            element_at(parameters, 'carrierFreqNrCell') as carrierFreqNrCell,
            element_at(parameters, 'quantityConfigId') as quantityConfigId,
            element_at(parameters, 'ssbDuration') as ssbDuration,
            element_at(parameters, 'ssbOffset') as ssbOffset,
            element_at(parameters, 'ssbPeriodicity') as ssbPeriodicity,
            element_at(parameters, 'ssbSubcarrierSpacing') as ssbSubcarrierSpacing,
            element_at(ids, 'MRBTS') as MRBTS_id,
            element_at(ids, 'LNBTS') as LNBTS_id
            from hive.network_data.cm
            where mo_class = 'ENDCDMEASCONF') ENDCDMEASCONF on (LNBTS.mrbts_id = ENDCDMEASCONF.mrbts_Id and LNBTS.LNBTS_Id = ENDCDMEASCONF.LNBTS_Id and LNBTS.raml_date=ENDCDMEASCONF.raml_date) left join


        (select mo_distname, raml_date, 
            element_at(parameters, 'adjGnbId') as adjGnbId,
            element_at(ids, 'MRBTS') as MRBTS_id,
            element_at(ids, 'LNBTS') as LNBTS_id
            from hive.network_data.cm
            where mo_class = 'LNADJGNB') LNADJGNB on (LNBTS.MRBTS_id = LNADJGNB.MRBTS_id AND LNBTS.LNBTS_id = LNADJGNB.LNBTS_Id and LNBTS.raml_date=LNADJGNB.raml_date) inner join
        

        (select mo_distname, raml_date, 
            element_at(parameters, 'name') as name,
            element_at(ids, 'NRBTS') as NRBTS_id
            from hive.network_data.cm
            where mo_class = 'NRBTS') NRBTS on (LNADJGNB.adjGnbId = NRBTS.NRBTS_id and NRBTS.raml_date=LNADJGNB.raml_date)
            
            
            
    """)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]
    anchor_sites = pd.DataFrame(data,columns=columns)


    anchor_sites.to_csv('anchor_sites.csv', index=False)


    return True

