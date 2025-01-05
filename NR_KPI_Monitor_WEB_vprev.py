import base64
import io
import os
import os.path, time
import json

from io import StringIO

import plotly.graph_objs as go


from datetime import date,timedelta, datetime
import plotly.express as px

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table



import pandas as pd
import numpy as np
import requests

import pytz 




from functools import reduce
import operator


from NR_Monitor_Functions import generate_alarm_report, NR_get_clusters, generate_anchor_sites


# S3 Connection

import boto3

# S3 Connection

s3_credentials = json.loads(str(os.environ['s3_credentials']))

s3_bucket = s3_credentials["bucket"]

s3 = boto3.session.Session().client(
    service_name='s3',
    aws_access_key_id=s3_credentials["access_key"],
    aws_secret_access_key=s3_credentials["secret_key"],
    endpoint_url=s3_credentials["endpoint"])



time_zone=os.getenv('time_zone')
pm_api=os.getenv('pm_api')

path = os.getcwd()
print(path)



from sqlalchemy import create_engine


time_zone=os.getenv('time_zone', 'UTC')

clickhouse_user=os.getenv('clickhouse_user')
clickhouse_pass=os.getenv('clickhouse_pass')
clickhouse_url=os.getenv('clickhouse_url')
clickhouse_port=os.getenv('clickhouse_port')
clickhouse_driver='http'


connection_string = f'clickhouse://{clickhouse_user}:{clickhouse_pass}@{clickhouse_url}:{clickhouse_port}'

conn_PM = create_engine(connection_string)




import requests
import json


#API Performance Monitor

api_url=f"http://{pm_api}/api/calculate_kpi"

# KPIS semanales cluster

kpis=['NR_5020d','NR_5080a','NR_5124b','NR_5152a']






#START DESIGNING WEB PAGE


tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


app = dash.Dash(__name__, external_stylesheets=external_stylesheets, title='5G Cluster Monitor')


styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll',
        'fontSize':12
    }
}


cur_version="1.5"

app.layout =html.Div([

  

    html.Div([

    html.Img(src=app.get_asset_url('Nokia-Logo.png'))

    ],
    style={'width': '25%', 'display': 'inline-block', 'margin-left':'20px', 'margin-top':'20px','verticalAlign': 'top'}),


    html.Div([
    html.H3(f"NR Cluster Online Monitor", id="sch_label", 
        style={
            'textAlign': 'left',
            'color': 'black'
        }, hidden=False)

    ],
    style={'width': '25%', 'display': 'inline-block', 'margin-left':'20px','verticalAlign': 'top'}),




    html.Div([
    html.H6("v"+str(cur_version), 
        style={
            'textAlign': 'right',
            'color': 'red'
        }, hidden=False)

    ],
    style={'width': '5%', 'display': 'inline-block','verticalAlign': 'top'}),

    html.Div([

        dcc.Loading(
            id="loading-1",
            children=[html.Div([html.Div(id="loading-output-1")])],
            type="default",
        )

    ],
    style={'width': '15%', 'display': 'inline-block'}), 


    html.Div([
    html.H6(f"Hora de Actualización: ", id="update_label", 
        style={
            'textAlign': 'left',
            'color': 'black'
        }, hidden=False)

    ],
    style={'width': '26%', 'display': 'inline-block', 'margin-left':'20px', 'float': 'right','verticalAlign': 'top'}),


    html.Hr(),


    html.Div([

        dcc.Dropdown(
            id="dropdown_region", 
            options=[
                {'label': x, 'value': x}
                for x in ['Centro','Costa','Noroccidente','Nororiente','Suroccidente']
            ],
            multi=False,
            placeholder="Región",
            clearable=True,
            disabled=False
        ),


    

        dcc.Graph(id='NR_5020d', style={"height": 270}),

        dcc.Graph(id='NR_5080a', style={"height": 270}),

        dcc.Graph(id='NR_5124b', style={"height": 270}),

        dcc.Graph(id='NR_5152a', style={"height": 270}),

        html.Button('Download RAW Data', id='raw_data_bt', disabled=False, style={'backgroundColor':'LightBlue','color': 'black'}),



    ],
    style={'width': '65%', 'float': 'left', 'display': 'inline-block','verticalAlign': 'top'}), 




    html.Div([


        dcc.Markdown("""
            Black List Sites Monitor:
        """),
        html.Pre(id='click-data-2', style=styles['pre']),



    

    dash_table.DataTable(
            id='table_black_list',
            columns=[{"name": i, "id": i} for i in ['Date','Site','NR_5152a']],
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'tomato',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    html.Hr(),


        dcc.Markdown("""
            Showing Offenders for:
        """),
        html.Pre(id='click-data', style=styles['pre']),



    

    dash_table.DataTable(
            id='table_offenders',
            columns=[{"name": i, "id": i} for i in ['cluster','Site','value']],
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'lightgrey',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    html.Hr(),

    html.Button('Download Cluster & KPI Offenders', id='avail_off_bt', disabled=False, style={'backgroundColor':'LightBlue','color': 'black'}),
    html.Hr(),
    #html.Button('Download Availability Offenders ALARMS', id='avail_alarms_bt', disabled=False, style={'backgroundColor':'tomato','color': 'black'}),
    html.Hr(),
    #html.Button('Download Anchor Sites ALARMS', id='anchor_alarms_bt', disabled=False, style={'backgroundColor':'tomato','color': 'black'}),

    html.Button('Download Offenders ALARMS Report', id='anchor_alarms_bt-rep', disabled=False, style={'backgroundColor':'tomato','color': 'black'}),


    ],

    style={'width': '15%', 'float': 'left', 'display': 'inline-block', 'margin-left':'30px','verticalAlign': 'top'}), 






    html.Div([




    dcc.Markdown("""
        __Last Hour Availability Offenders:__
    """),


    dash_table.DataTable(
            id='table_no_traffic',
            columns=[{"name": i, "id": i} for i in ['Date','cluster','Site','NR_5152a']], 
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'orange',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    #html.Hr(),


    dcc.Markdown("""
        __2nd Last Hour Availability Offenders:__
    """),


    dash_table.DataTable(
            id='table_no_traffic2',
            columns=[{"name": i, "id": i} for i in ['Date','cluster','Site','NR_5152a']], 
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'orange',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    #html.Hr(),


    dcc.Markdown("""
        __3rd Last Hour Availability Offenders:__
    """),


    dash_table.DataTable(
            id='table_no_traffic3',
            columns=[{"name": i, "id": i} for i in ['Date','cluster','Site','NR_5152a']], 
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'orange',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    #html.Hr(),


    dcc.Markdown("""
        __4th Last Hour Availability Offenders:__
    """),


    dash_table.DataTable(
            id='table_no_traffic4',
            columns=[{"name": i, "id": i} for i in ['Date','cluster','Site','NR_5152a']], 
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'orange',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    #html.Hr(),


    dcc.Markdown("""
        __5th Last Hour Availability Offenders:__
    """),


    dash_table.DataTable(
            id='table_no_traffic5',
            columns=[{"name": i, "id": i} for i in ['Date','cluster','Site','NR_5152a']], 
            style_cell={'textAlign': 'center', 'fontSize':12},
            style_header={
                            'backgroundColor': 'orange',
                            'fontWeight': 'bold', 'textAlign': 'center'
                        },

        )   ,

    #html.Hr(),


    html.Button('Download Avail Offenders History', id='avail_off_hist_bt', disabled=False, style={'backgroundColor':'LightBlue','color': 'black'}),



    ],

    style={'width': '15%', 'float': 'left', 'display': 'inline-block', 'margin-left':'20px','verticalAlign': 'top'}), 




    html.Div(id='output-data-upload', hidden=True),
    html.Div(id='output-data-upload2', hidden=True),

    dcc.Store(id='selected_region'),


    dcc.Interval(id='interval-update', interval=600000, n_intervals=0),


    dcc.Download(id="download-off"),
    dcc.Download(id="download-raw"),
    dcc.Download(id="download-avail-off"),
    dcc.Download(id="download-avail-alarms"),
    dcc.Download(id="download-anchor-alarms"),
    dcc.Download(id="download-anchor-alarms-rep")


 
        ])

@app.callback(
    Output('selected_region','data'),
    Input('dropdown_region', 'value'), 
    prevent_initial_call=True,
    
    )
def execute_functions(task_region):

    #print(task_region)
    return task_region




@app.callback(
    Output('click-data', 'children'),
    Output("table_offenders","data"),
    Input('NR_5020d', 'clickData'),
    Input('NR_5080a', 'clickData'),
    Input('NR_5124b', 'clickData'),
    Input('NR_5152a', 'clickData'))
def display_click_data(d_NR_5020d,d_NR_5080a,d_NR_5124b,d_NR_5152a):

    cluster=""
    kpi_graph=""

    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    #print(sel_sites_ws)

    if 'NR_5020d' in changed_id:
        cluster=d_NR_5020d['points'][0]['label']
        kpi_graph='NR_5020d'

    elif 'NR_5080a' in changed_id:
        cluster=d_NR_5080a['points'][0]['label']
        kpi_graph='NR_5080a'

    elif 'NR_5124b' in changed_id:
        cluster=d_NR_5124b['points'][0]['label']
        kpi_graph='NR_5124b'

    elif 'NR_5152a' in changed_id:
        cluster=d_NR_5152a['points'][0]['label']
        kpi_graph='NR_5152a'


    offenders=pd.read_csv('all_offenders.csv')

    cur_off=offenders[(offenders['cluster']==cluster) & (offenders['kpi']==kpi_graph)]

    table_data=cur_off[['cluster','Site','value']].to_dict('records')

    #print(table_data)

    return f"Cluster: {cluster}  ->  {kpi_graph}", table_data




@app.callback(
    Output('NR_5020d', 'figure'),
    Output('NR_5080a', 'figure'),
    Output('NR_5124b', 'figure'),
    Output('NR_5152a', 'figure'),
    Output('update_label', 'children'),
    Output("loading-output-1", "children"),
    Output("table_no_traffic","data"),
    Output("table_no_traffic2","data"),
    Output("table_no_traffic3","data"),
    Output("table_no_traffic4","data"),
    Output("table_no_traffic5","data"),
    Output("table_black_list","data"),
    Input('interval-update', 'n_intervals'),
    Input('selected_region','data'),
    )
def execute_functions(n, selected_region):


# Crear anchor sites si pasa 1 día



    modif_date=datetime.fromtimestamp(os.path.getctime('anchor_sites.csv'))

    survival_time=datetime.now()-modif_date

    if survival_time.days==1:

        generate_anchor_sites()

    else:

        pass



#Actualizar lista de procesos
    
    dummy_df=pd.DataFrame()

    dfs_hour=[dummy_df,dummy_df,dummy_df,dummy_df,dummy_df]

    avail_off=pd.DataFrame(columns=['Date','cluster','Site','NR_5152a'])

    timeZ_BOG = pytz.timezone(time_zone) 




    try:

        status=NR_get_clusters(s3, s3_bucket)


        kpis=['NR_5020d','NR_5080a','NR_5124b','NR_5152a']


        tasks_all=pd.read_csv('scheduled_tasks.csv')

        cluster_start=tasks_all[['task_name','start_date']].copy()

        cluster_start['elapsed_days']=cluster_start['start_date'].apply(lambda x: (datetime.now()-datetime.strptime(x, '%d/%m/%y')).days)
        cluster_start['elapsed_weeks']=cluster_start['elapsed_days'].apply(lambda x: f"{x//7}W{x%7}d" if x>=0 else f"{x}d")



        if selected_region!=None:
            tasks=tasks_all[tasks_all['region']==selected_region]
        else:
            tasks=tasks_all
        


        KPI_all=pd.DataFrame()
        KPI_Site_Day_All=pd.DataFrame()

        # encontrar lunes

        now = datetime.now()
        monday = now - timedelta(days = now.weekday())
        start_time=monday.strftime("%Y-%m-%d")
        stop_time=now.strftime("%Y-%m-%d")

        #print('lunes:',start_time, stop_time)

        #start_time="2023-12-04"
        #stop_time="2023-12-07"

        all_sites=[]


        modif_date=datetime.fromtimestamp(os.path.getctime('dummy_trigger.csv'))

        survival_time=datetime.now()-modif_date

        if survival_time.minutes>15:

            pd.DataFrame(columns=['x','y']).to_csv('dummy_trigger.csv')



            
            for i, j in tasks.iterrows():


                task_start=cluster_start[cluster_start['task_name']==j['task_name']]


                #print(j['task_name'] )


                sites=j['task_WS'].replace("'","").replace("[","").replace("]","").split(', ')


                all_sites=all_sites+sites

                obj = {
                "tech": "NR",
                "kpi_id": (",").join(kpis),
                "site": (",").join(sites),
                "start_date": start_time,
                "end_date": stop_time,   
                "timeAgg": "Whole Period",
                "timeZone": time_zone,
                "exclusion_hour": "22,23,0,1,2,3,4,5",
                }


                x = requests.post(api_url, json = obj)
                data = json.loads(x.text)
                KPI_Site_Cluster = pd.DataFrame.from_dict(data).dropna(subset=['NR_5152a'])

                


                obj_site = {
                "tech": "NR",
                "kpi_id": (",").join(kpis),
                "site": (",").join(sites),
                "start_date": start_time,
                "end_date": stop_time,   
                "timeAgg": "Hour",
                "timeZone": time_zone,
                "groupby": "site_name",
                "exclusion_hour": "22,23,0,1,2,3,4,5",
                }



                x = requests.post(api_url, json = obj_site)
                data = json.loads(x.text)
                KPI_Site_Day=pd.DataFrame.from_dict(data).dropna(subset=['NR_5152a'])
                KPI_Site_Day['cluster']=j['task_name']

                KPI_Site_Day_All=pd.concat([KPI_Site_Day_All,KPI_Site_Day])




                KPI_Site_Cluster['cluster']=j['task_name']

                KPI_Site_Cluster['elapsed_weeks']=task_start['elapsed_weeks'].values[0]

                KPI_all=pd.concat([KPI_all,KPI_Site_Cluster])




            KPI_all_ini=KPI_all.copy()

            KPI_Site_Day_All.to_csv('NR_Monitor_KPI_Site_Day.csv', index=False)

            #print(KPI_all_ini)


            ### Generate plots

            kpis_th=[('NR_5020d',65,'5G NSA Non Stand Alone call accessibility, 5G side'),('NR_5080a',0,'5G NSA PDCP SDU data volume transmitted without repetitions in DL'),
                    ('NR_5124b',0,'5G NSA Average number of NSA users in selected area'),('NR_5152a',99,'5G Cell availability ratio excluding planned unavailability periods')]

            plot_list=[]

            for kpi in kpis_th:

                    data_plot=KPI_all_ini.sort_values(by=kpi[0], ascending=False).fillna(0)

                    if data_plot.shape[0]>0:

                        y=data_plot[kpi[0]].values

                        color=np.array(['green']*data_plot.shape[0])



                        try:
                            color[y<=kpi[1]]='red'
                        except:
                            pass

                        #color[y>kpi[1]]='green'


                        GS=100
                        figure=px.bar(data_plot,y=kpi[0],x='cluster', color=color, color_discrete_map={"red":"red", "green":"#90EE90"}, hover_data=["elapsed_weeks"])


                        anot=data_plot
                        anot
                        for i,j in anot.iterrows():

                                figure.add_annotation(x=j['cluster'], y=j[kpi[0]]*0.8,
                                        text=j['elapsed_weeks'],
                                        showarrow=False,textangle=-90,
                                        font=dict(size=9, color="black"))
                                
                                
                        anot=data_plot[data_plot[kpi[0]]<=kpi[1]]
                        anot
                        for i,j in anot.iterrows():

                                figure.add_annotation(x=j['cluster'], y=j[kpi[0]],
                                        text="*",
                                        showarrow=True,
                                        arrowhead=5)


                        figure.update_layout(showlegend=False, height=270, xaxis_title=None, margin=dict(l=10, r=20, t=30, b=15),
                                            title=f"{kpi[2]} - Target > {kpi[1]}",
                                                font=dict(
                                                        size=9,
                                                        color="black"))

                        figure.update_xaxes(tickangle=-45, tickfont=dict( color='black', size=10))


                        #figure.update_layout(yaxis_range=[-0.0001,100])



                        
                        plot_list.append(figure)
                    else:
                        plot_list=[dash.no_update,dash.no_update,dash.no_update,dash.no_update]


            ## Convertir data tasks en tabla de sitios por cluster

            sites_cluster=pd.DataFrame()

            for i,j in tasks[['task_name','task_WS']].iterrows():


                sites=j['task_WS'].replace("'","").replace("[","").replace("]","").split(', ')

                sites_cluster_temp=pd.DataFrame()
                sites_cluster_temp['Site']=sites
                sites_cluster_temp['cluster']=j['task_name']

                sites_cluster=pd.concat([sites_cluster,sites_cluster_temp])

            



            all_offenders=pd.DataFrame()


            for kpi in kpis_th:
                
                Cluster_site_offenders=KPI_all_ini[(KPI_all_ini[kpi[0]]<=kpi[1])]

                sites_in_cluster_off=Cluster_site_offenders.merge(sites_cluster, on='cluster')[['cluster','Site']]

                if sites_in_cluster_off.shape[0]>0:



                    obj = {
                    "tech": "NR",
                    "kpi_id": kpi[0],
                    "site": (",").join(list(sites_in_cluster_off['Site'])),
                    "start_date": start_time,
                    "end_date": stop_time,   
                    "timeAgg": "Whole Period",
                    "groupby": "site_name",
                    "timeZone": time_zone,
                    "exclusion_hour": "22,23,0,1,2,3,4,5",
                    }



                    x = requests.post(api_url, json = obj)
                    data = json.loads(x.text)
                    KPI_site_offender = pd.DataFrame.from_dict(data).merge(sites_cluster, on='Site').sort_values(by=kpi[0])

                    KPI_site_offender=KPI_site_offender[KPI_site_offender[kpi[0]]<=kpi[1]]

                    KPI_site_offender.rename(columns={kpi[0]:'value'}, inplace=True)

                    KPI_site_offender['kpi']=kpi[0]

                    all_offenders=pd.concat([all_offenders,KPI_site_offender])




                if all_offenders.shape[0]>0:
                    all_offenders['value']=all_offenders['value'].apply(lambda x: round(x, 4))

                    all_offenders[['cluster','Site', 'kpi', 'value']].to_csv('all_offenders.csv', index=False)

                else:
                    pd.DataFrame(columns=['cluster','Site', 'kpi', 'value']).to_csv('all_offenders.csv', index=False)

            # Ofensores de disponibilidad última hora

            obj = {
            "tech": "NR",
            "kpi_id": "NR_5152a",
            "site": (",").join(list(sites_in_cluster_off['Site'])),
            "start_date": stop_time,
            "end_date": stop_time,   
            "timeAgg": "Hour",
            "groupby": "site_name",
            "timeZone": time_zone,
            "exclusion_hour": "22,23,0,1,2,3,4,5",
            }

            x = requests.post(api_url, json = obj)
            data = json.loads(x.text)
            KPI_site_offender_hour = pd.DataFrame.from_dict(data).merge(sites_cluster, on='Site').sort_values(by='Date', ascending=False)

            KPI_site_offender_hour['NR_5152a']=KPI_site_offender_hour['NR_5152a'].apply(lambda x: round(x, 4) if x!=None else None )


            avail_off=KPI_site_offender_hour[KPI_site_offender_hour['NR_5152a']<99]



            hours_to_show=sorted(list(set(KPI_site_offender_hour['Date'])))[-5:]

            dfs_idx=0

            for hour in hours_to_show:

                cur_data=avail_off[avail_off['Date']==hour]

                dfs_hour[dfs_idx]=cur_data

                #dfs_hour.append(cur_data)

                dfs_idx=dfs_idx+1


            #Monitor Black List
            
            black_list=pd.read_csv('nr_black_list.csv') 


            obj = {
            "tech": "NR",
            "kpi_id": "NR_5152a",
            "site": (",").join(list(black_list['Sitio'].values)),
            "start_date": (datetime.today()-timedelta(days=0)).strftime("%Y-%m-%d"),
            "end_date": (datetime.today()-timedelta(days=0)).strftime("%Y-%m-%d"),   
            "timeAgg": "Hour",
            "timeZone": time_zone,
            "groupby": "site_name"
            }

            x = requests.post(api_url, json = obj)
            data = json.loads(x.text)
            avail_black = pd.DataFrame.from_dict(data).dropna()

            avail_black['NR_5152a']=avail_black['NR_5152a'].apply(lambda x: round(x, 2) if x!=None else None )

            black_info=pd.DataFrame(columns=['Date','NR_5152a','Site'])

            for delta in [2,1,0]:
                data_hour=(datetime.now(pytz.timezone(time_zone))-timedelta(hours=delta)).strftime("%Y-%m-%d %H:00:00")

                data_bl=avail_black[avail_black['Date']==data_hour]

                black_info=pd.concat([black_info,data_bl])



            error_str=''



        else:

            plot_list=[dash.no_update,dash.no_update,dash.no_update,dash.no_update]

    except Exception as error_monitor:
        status_nqi=False
        error_str="\nError at line: " + str(error_monitor.__traceback__.tb_lineno) + ": " + type(error_monitor).__name__ + "  " + str(error_monitor)
        print(error_str)
        pd.DataFrame(columns=['cluster','Site', 'kpi', 'value']).to_csv('all_offenders.csv', index=False)

        plot_list=[dash.no_update,dash.no_update,dash.no_update,dash.no_update]




 
        

    return  plot_list[0],plot_list[1],plot_list[2],plot_list[3],"Last Update:  "+datetime.now(timeZ_BOG).strftime("%Y-%m-%d %HH:%MM")+error_str, "", dfs_hour[4].to_dict('records'), \
    dfs_hour[3].to_dict('records'), dfs_hour[2].to_dict('records'), dfs_hour[1].to_dict('records'), dfs_hour[0].to_dict('records'), black_info.to_dict('records')




@app.callback(
    Output("download-off", "data"),
    Input('avail_off_bt', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


    download_file=dcc.send_file('all_offenders.csv')

    return  download_file


@app.callback(
    Output("download-raw", "data"),
    Input('raw_data_bt', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


    download_file=dcc.send_file('NR_Monitor_KPI_Site_Day.csv')

    return  download_file



@app.callback(
    Output("download-avail-off", "data"),
    Input('avail_off_hist_bt', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


    data_hist=pd.read_csv('NR_Monitor_KPI_Site_Day.csv')[['Date','Site','NR_5152a','cluster']]

    data_hist.sort_values(by='Date', ascending=False)



    data_hist=data_hist[data_hist['NR_5152a']<99]

    data_hist.to_csv('Avail_Offenders_History.csv', index=False)


    download_file=dcc.send_file('Avail_Offenders_History.csv')


    return  download_file


@app.callback(
    Output("download-avail-alarms", "data"),
    Input('avail_alarms_bt', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


    data_hist=pd.read_csv('NR_Monitor_KPI_Site_Day.csv')[['Date','Site','NR_5152a','cluster']]

    data_hist.sort_values(by='Date', ascending=False)



    data_hist=data_hist[data_hist['NR_5152a']<99]

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

    off_alarms.to_csv('Offenders_Alarms.csv', index=False)

    download_file=dcc.send_file('Offenders_Alarms.csv')



    return  download_file




@app.callback(
    Output("download-anchor-alarms", "data"),
    Input('anchor_alarms_bt', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


# Alarmas de Anchor Sites

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

    anchor_alarms.to_csv('Anchor_Alarms.csv', index=False)

    download_file=dcc.send_file('Anchor_Alarms.csv')   



    return  download_file



@app.callback(
    Output("download-anchor-alarms-rep", "data"),
    Input('anchor_alarms_bt-rep', 'n_clicks'), 
    prevent_initial_call=True )
def execute_functions(n):


    rand_str=generate_alarm_report(conn_PM)


    download_file=dcc.send_file(f'Escalamientos_5G_{rand_str}.xlsx')   


    os.remove(f'Escalamientos_5G_{rand_str}.xlsx')




    return  download_file

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
