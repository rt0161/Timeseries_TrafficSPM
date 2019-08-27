#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm
from fastparquet import ParquetFile, write
import time
import glob
import matplotlib.pyplot as plt
import numpy as np

# bokeh plottings
from bokeh.io import show, output_file
from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, Plot,DatetimeTickFormatter,FixedTicker

# sumup the readfile action into a function:
def readSPM_fldin(Datapath):
    """ function works as to read all .csv files(SPM) from a specific folder """
    """ Data path has to be set before using this function"""
    """ Example Datapath setting: Datapath='/Users/z0043H7B/Tien/SPM Big Data Challenge/Data/Fremont/fremont_capitol/'"""
    """ Returns a concatenated dataframe including all the data from the SPM read"""
    
    # detect empty files
    def is_non_zero_file(filen):  
        return True if os.path.isfile(filen) and os.path.getsize(filen) > 16 else False
    
    os.chdir(Datapath)
    os.system('ls SIEM*.csv !> readf.txt')
    with open('readf.txt','r') as rf:
        # check the file size before loading, take care of looping over the the empty csv
        inputf=rf.readline().split('\n')[0]
        if is_non_zero_file(inputf):
            spm_full= pd.read_csv(inputf,encoding='utf-8',parse_dates=[0],index_col=0,header=None,skiprows=1,names=[0,1,2,3])
        else: 
            print("empty first file.")
            return

        for i,line in enumerate(tqdm(rf)):
            if i==0:continue
            inputf=line.split('\n')[0]
            if is_non_zero_file(inputf):
                spm1=pd.read_csv(inputf,encoding='utf-8',parse_dates=[0],index_col=0,header=None,skiprows=1,names=[0,1,2,3])
                spm_full = pd.concat([spm_full,spm1])
                print(inputf+'....')
            else:
                continue
    return spm_full


def fastInSPM_fldin(Datapath):
    """ function should be used after all .csv files(SPM) from a specific folder are concatenated by bash script
    (clean_concat.sh) Data path has to be set before using this function. 
    Example Datapath setting: Datapath='/Users/z0043H7B/Tien/SPM Big Data Challenge/Data/Fremont/fremont_capitol/
    Reading in the concatenated csv and save to a parquet file to the folder for later use.
    Returns a concatenated dataframe including all the data from the SPM read"""
    
    os.chdir(Datapath)
    # if exist parquet file, load it, return and exist
    files =glob.glob('full*.parq')
    if len(files) !=0:
        # read from parquet file 
        pf = ParquetFile('fullspm.parq')
        spm_full=pf.to_pandas()
        return spm_full
    colname = ['time','events','device','type']
    dtypes = {'events':'int32','device':'int32'}
    tic = time.clock()
    spm_full = pd.read_csv('all.csv',encoding='utf-8',parse_dates=True,index_col=0,header=None,names=colname)
    toc = time.clock()
    print("time spent loading.....",toc-tic," sec")
    # save the file into parquet 

    write('fullspm.parq',spm_full)
    # read back: pf = ParquetFile('devon_fullspm.parq'); df=pf.to_pandas()
    
    return spm_full

# In[6]:


# a function for extract one detector --->>> Advance this into a parser based on different 
def getdtrevt(data, num_det):
    """input full concatenated time series from SPM,""" 
    """find the specific detector events """
    """return detector time series in dataframe as subset """
    sub=data.loc[(data.iloc[:,0].isin([81,82,83,84,85,86,87,88]))&(data.iloc[:,1].isin([num_det]))]
    
    return sub


# In[8]:


# a function to parse different devices versus their own events
def eventParse(data,indf_n):
    """This is a datetime dataframe parser based on different but specific SPM phase or detector types"""
    """Takes in the dataframe with datatime index that was concatenated from readSPM_fldin"""
    """The function parse original dataframe into two different dataframes, one contains the specified type of events (Phase, Vehicle det., Ped det., Overalls or Split)"""
    """another(or maybe 2) contains that specific type of events associated with particular phase/or detector number"""
    
    cat_Ph = [x for x in range(0,21)] +[41,42,43,44,46,47]+[y for y in range(50,61)] #phase related events
    cat_Ped = [x for x in range(21,31)] +[45,48,49]+[y for y in range(67,72)] +[z for z in range(89,93)]   #pedestrian related events
    cat_Ovr = [x for x in range(61,67)] + [y for y in range(71,81)]  #OVerall events
    cat_Vd = [x for x in range(80,89)] + [y for y in range(1110,1119)]+ [z for z in range(93,101)]  #Vehical detector events
    cat_Prem = [x for x in range(101,112)] + [y for y in range(116,131)] #Preempt events
    cat_TSP = [x for x in range(112,116)]  # TSP events(?)
    cat_Splt = [x for x in range(134,149)] # Split events
    cat_Cord = [131,132,133,151]+ [x for x in range(152,171)] #Coordination events ** Note, can return multiple dataframes after splitting several types
    cat_Test = [171,172]  #Test events
    cat_NTCIP = [173.174,175] # Unit events
    cat_Spec = [176,177]      # special events
    cat_OnOff = [178,179,180]  # Interval Event, Stop Time, Controller clock event(only on/offs)
    cat_Powr =[181,182]

        # define type of events
    eventabl ={ 1:cat_Ph,
                2:cat_Ped,
                3:cat_Ovr,
                4:cat_Vd,
                5:cat_Prem,
                6:cat_TSP,
                7:cat_Splt,
                8:cat_Cord,
                9:cat_Test,
                10:cat_NTCIP,
                11:cat_Spec,
                12:cat_OnOff,
                13:cat_Powr}
    try: int(indf_n)
    except ValueError:  return 'Not specify the event type....Please redo.'
    sel_evnt = eventabl[indf_n]
    
    #first filter out the specific events
    tmp_df = data.loc[data.iloc[:,0].isin(sel_evnt)]
        
        # check if further filtering is needed.*** not needed at this time.
        
    return tmp_df


# In[ ]:


# create On-duration and Off-duration

def getOnOffDur(df):
    """ function works to find detector on/off time duration and add it as a new column to the dataframe"""
    """ referencing only event 82 and 81"""
    """ prerequsit: were not dump datetime index to date column"""
    """ return another """
    # create detector-on timemark
    chk_cyc = df.events.values
    rep = checkRep(chk_cyc)
    indx = [x[0]-1 for x in rep]  # getting the index of the rows which should be dropped
    if len(indx)!=0: print("found broken cycles: ",df.iloc[indx])
    # drop off the rows that are duplicated
    df.drop(df.index[indx], inplace = True)
    a=pd.to_datetime(df.loc[df.iloc[:,0]==82].index) #On
    b=pd.to_datetime(df.loc[df.iloc[:,0]==81].index) #Off
    if a[0]> b[0]: b = b[1:] # Off event first and then on, need to remove first off and then start with on
        
    if(len(a)>len(b)): a=a[:len(b)]
    elif(len(b)>len(a)): b=b[:len(a)]
    c=b-a  # create a third timestamp of 'On' time duration
    dummy1 = pd.DataFrame(c.total_seconds(),index=b)
    dummy1.columns=['On duration','date']
    d=a[1:]-b[:-1] #Off duration
    dummy2 = pd.DataFrame(d.total_seconds(),index=a[1:])
    dummy2['date']=pd.to_datetime(dummy2.index)
    dummy2.columns=['Off duration','date']
    final = pd.merge(dummy1,dummy2, how='outer', on='date',sort=True)
    final = final.set_index('date', drop=False)
    return final


# In[ ]:


# create accumulated car counts within Ns moving window

def getCounts(df,n):
    """ This function calculates the accumulated car counts within a user-defined period N seconds moving window"""
    """ Input is the concatenated dataframe, with timedate as index, output is dataframe of car counts within that N seconds moving window"""
    timw = str(n)+'s'
    df['counts']=df['On duration'].rolling(timw,min_periods= 1, closed='both').count()
    return df

# further separate dataframe into specific id (of phase, detector# or channel#... etc)
def parse_more(df,coln,idn):
    """ Input : (1)datetime indexed dataframe, with no date(datetime) column inserted yet """
    """ (2) number of the column to be parse on, 0 is by event and 1 is by id# """
    """ (3) a list of id numbers or event number  that usr wants to include"""
    """ Output : parsed dataframe with no date column inserted"""
    new_df = df.loc[df.iloc[:,coln].isin(idn)]
    return new_df

def getmov_avg(df,in_col,out_col,timw):
    """ This function adds the column (name as out_col, string) to the dataframe, as moving average of the """
    """ in_col value in a moving time window specified by timw(string). Note that the time window counts """       """ from the right of the timestamp"""
    timws = str(timw)+'s'
    df[out_col]=df[in_col].rolling(timws).mean()
    
    return df


def getcounts(df,col,timw):
    """ Create a new column called 'counts' to the dataframe, with a specified time windows (seconds), """
    """ calculate specific detector events count within this window. (specified by parameter col(string))"""
    """ return the new dataframe. Note input dataframe has to be timedate indexed , the returned column is"""
    """ right aligned on the particular timestamp"""
    timws = str(timw)+'s'
    df['counts']=df[col].rolling(timw,min_periods= 1).count()
    
    return df

def cntPhases(df):
    """ filter the fully concatenated time-series, find out how many phases is used,
    return the total number of phases, and the list of the phase number"""
    dummy=eventParse(df,1)
    phaselist=dummy.iloc[:,1].value_counts().index.values
    totalphs=len(phaselist)

    return totalphs,phaselist

def cntDetN(df):
    """ filter the fully concatenated time-series, find out how many vehicle detectors are used,
    return the total number of detectors, and the list of detector number"""
    envtype = [x for x in range(80,89)] + [y for y in range(1110,1119)]
    dummy=df.iloc[:,0].isin(envtype)
    detlist=df.loc[dummy].iloc[:,1].value_counts().index.values
    totaldetn=len(detlist)

    return totaldetn,detlist

def addPrdTime(df):
    """ with a dataframe indexed on datetime, contains a column of 'date', function adds month,day,hour and 
    day of the week"""
    df['month']=df.date.dt.month
    df['day']=df.date.dt.day
    df['hour']=df.date.dt.hour
    df['minute']=df.date.dt.minute
    df['day-of-week']=df.date.dt.weekday
    
    return df
    
def Filt_GreenTime_Det(inp_df, ref_df):
    """ This function filters the dataframe which contains single detector, pairs it with the assigned phase 
        data, filter out the records for only detector triggers during green (among event1 and 7)"""
    #get column names
    col=list(inp_df.columns.values)
    out_df=pd.DataFrame(columns=col)
    # get starttime/end time (event=1)
    # prevent switch out of cyclical change between 1 and 11
    chk_cyc = ref_df.events.values
    # check broken cyclical patter locations
    rep = checkRep(chk_cyc)
    indx = [x[0]-1 for x in rep]  # getting the index of the rows which should be dropped
    ref_df.drop(ref_df.index[indx], inplace = True)
    # drop the rows where omit had messed with the repetition
    
    stt = pd.to_datetime(ref_df.loc[ref_df.iloc[:,0]==1].index)
    edt = pd.to_datetime(ref_df.loc[ref_df.iloc[:,0]==11].index)

    
    for i in range(len(stt)):
        tmp_splt = inp_df[stt[i]:edt[i]]
        out_df = pd.concat([out_df,tmp_splt])
        
    return out_df

def pltDistrDur(df,column,detn,dtime,binn):
    """function plots the Distribution of the values of detector On/Off duration. 
        Input is dataframe, column name to plot(string), detector number(int), starting time notes 
        (for noting the file name specifically, use a string e.g. 10_18_2019). And the number of bins
        for optimization. Figure plotted outputs as a PNG file to the code running folder."""
    # Generate data on commute times.
    size, scale = 878, 10
    commutes = df[column]
    commutes.plot.hist(grid=True, bins=binn, rwidth=0.9,color='#607c8e')
    strtitl=column+' for det '+str(detn)+'at time '+dtime
    plt.title(strtitl)
    plt.xlabel(column+' (sec)')
    plt.ylabel('Counts')
    plt.grid(axis='y', alpha=0.75)
    outpfn = ''.join(column.split())+'_det'+str(detn)+'_'+dtime+'.png' 
    plt.savefig(outpfn)
    
def pltTMS(df,column,detn,dtime):
    """function plots the Distribution of the values of detector On/Off duration. 
        Input is dataframe, column name to plot(string), detector number(int), starting time notes 
        (for noting the file name specifically, use a string e.g. 10_18_2019). And the number of bins
        for optimization. Figure plotted outputs as a PNG file to the code running folder.
        dataframe requirement: must have a datetime column 'date'."""
    fig, ax = plt.subplots(figsize=(20,6))
    #plt.plot(sub_onONdur['date'], sub_onONdur['On duration'],'.')
    plt.plot(df['date'], df[column],'.',color='g')
    ax = plt.gca()

    plt.gcf().autofmt_xdate() # Rotation
    strtitl=column+' for det '+str(detn)+'at time '+dtime
    plt.title(strtitl)
    plt.ylabel('Counts')
    plt.grid(axis='y', alpha=0.75)
    outpfn = ''.join(column.split())+'_det'+str(detn)+'_'+dtime+'.png' 
    plt.savefig(outpfn)
    

def plt3in1(df,detn,dtime):
    plt.subplot(221)
    return
    
def checkRep(arr):
    """This is a small tool to check a repetitive array for two numbers (e.g. [1,5,1,5,1,5,1,1,5,..]), 
    At which point that the repetition is broken. Function takes in the array/list, returns a list
    of the locations(index), and the values[1 or 5], as in e.g. [[2781,1],[104097,1],...]"""
    rep_num=[]
    for i,item in enumerate(arr):
        if i==0: start = item
        if i>0 :
            cur = item
            if cur != start:
                start = cur
                continue
            else: 
                #print(i,item)
                rep_num+=[[i,item]]
                start = cur
    return rep_num


def out_efdetnum(df):
    """Input full concatenated dataframe of all records, Output a list of the detector numbers that are good in 
    counts"""
    nDet,det_list=cntDetN(df)
    effdetlist=[]
    for det in det_list:
        detct=df.loc[(df.iloc[:,0].isin([81,82])) & (df.iloc[:,1]==det)].count()[0]
        if detct>1000: effdetlist+=[det]
    return effdetlist

def dfslc_handl_prd(df,std,prd):
    """split the main spm into some time period, the inputs are : start time(std) in formats of same as 
    2019-05-09 15:06:30.30, granularity down to day, the period that user specifies, like for e.g. 5days or 
    1 days 06:05:01.00003, or 15 us. see documentation on 
    https://pandas.pydata.org/pandas-docs/version/0.24.2/reference/api/pandas.to_timedelta.html"""
    stt=df.index[0]
    std = pd.Timestamp(stt.year,stt.month,stt.day)
    sub = df[str(std):str(std+pd.to_timedelta(prd))]
    
    return sub

def gen_onoff4plt(df,hd,tail):
    """ Need to input only the dataframe that filtered to only single device, containing only on-offs
    of that device. Inputs the event code for ONs(hd) and the OFFs(tail). This function takes care
    of irregular cycle outs of the on-off pairs, and the start and tail of the ON-OFFs to ensure 
    pairs are proper. Returns the starts(st_d) and ends(et_d) of the specific events for the time span."""
    chk_cyc = df.events.values
     # check broken cyclical patter locations
    rep = checkRep(chk_cyc)
    indx = [x[0]-1 for x in rep]  # getting the index of the rows which should be dropped
    if len(indx)!=0: print("found broken cycles: ",df.iloc[indx])
    # drop off the rows that are duplicated
    df.drop(df.index[indx], inplace = True)
    # check for that the ons' always get before offs.
    st_d = pd.to_datetime(df.loc[df.iloc[:,0]==hd].index) # the timestamps of the on-s
    et_d = pd.to_datetime(df.loc[df.iloc[:,0]==tail].index) # the timestamps for off-s
    # ensure on starts before off
    if et_d[0]<st_d[0]:
        print("data first starts with off event. First off signal is ignored!")
        print(et_d[0],"is skipped.....")
        et_d=et_d[1:] 
    if len(et_d)> len(st_d): 
        print("more off events are trailing, ignored:")
        print(et_d[len(st_d)-1:])
    et_d=et_d[:len(st_d)]
    
    return st_d, et_d

def gen_dtticker(df, mjfreq, mnfreq):
    """takes in the dataframe and then generate the proper time ticker for plotting in bokeh,
    Use inputs for major tick frequency(e.g. '10D'=10 days, '600S'=600 sec,..), and minor tick locations.
    see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases.
    returns the major and minor tick locations"""
    stt=df.index[0] # starttime mark
    stt_inp = pd.Timestamp(stt.year,stt.month,stt.day,stt.hour)
    edt=df.index[len(df.index)-1]
    edt_inp = pd.Timestamp(edt.year,edt.month,edt.day,edt.hour)

    labl_mnr=(pd.DataFrame(columns=['NULL'], index=pd.date_range(stt_inp,edt_inp,freq=mnfreq))
       .index.strftime('%Y-%m-%dT%H:%M:%S'))
    labl_mnr=pd.to_datetime(labl_mnr)
    minorticks = [x.to_datetime64().astype('datetime64[ms]').view(np.int64) for x in labl_mnr]

    labl_mjr=(pd.DataFrame(columns=['NULL'], index=pd.date_range(stt_inp,edt_inp,freq=mjfreq))
       .index.strftime('%Y-%m-%dT%H:%M:%S'))
    labl_mjr=pd.to_datetime(labl_mjr)
    majorticks = [x.to_datetime64().astype('datetime64[ms]').view(np.int64) for x in labl_mjr]
    
    return majorticks, minorticks

def plt_ints_cord(spm,sttime,prd,title):
    """ This plotting tool plots the spm information including detector triggering, phase signals to a single 
    plot, with selected time period from start-time for a selected period following start-time.
    plots out a html file to the data folder. Please specify intersection name to title"""
    # prepare dataframe for input: phases
    df = dfslc_handl_prd(spm,sttime,prd) # filter down to particular time frame
    namefig = ''.join(sttime.split(':')[0].split(' '))+'_'+prd+'.html'
    # get number of phases to plot
    detlst = out_efdetnum(df) 
    totalphs,phaselist=cntPhases(df)
    
    
    # setup figure
    output_file(namefig)
    # setup size 
    p = figure(title=title,plot_width=1400, plot_height=400)
    
    # setup tickers
    mjtick, mntick= gen_dtticker(df, '15D', '60T')
    
    dummyphs=eventParse(df,1)
    for phs in sorted(phaselist):
        spm_phs = parse_more(dummyphs,1,[phs]) # get down to the particular phase but with all phs events
        spm_phsg = parse_more(spm_phs,0,[1,7]) # get down to green events
        
        print("working on phase:",phs,"....")
        # get phase green time stamps
        st_d, et_d = gen_onoff4plt(spm_phsg,1,7)
        y=[phs for x in range(len(st_d))]
        # prepare and plot On-duration times for this detector
        p.segment(x0=st_d, y0=y, x1=et_d, y1=y, line_color="#2a6b2f", line_width=7)
        # get phase yellow time stamps
        spm_phsy = parse_more(spm_phs,0,[8,9]) # get down to green events
        st_d, et_d = gen_onoff4plt(spm_phsy,8,9)
        y=[phs for x in range(len(st_d))]
        p.segment(x0=st_d, y0=y, x1=et_d, y1=y, line_color="#fcee4c", line_width=7)
        # get phase red time stamps
        spm_phsr = parse_more(spm_phs,0,[10,1]) # get down to red events
        st_d, et_d = gen_onoff4plt(spm_phsr,10,1)
        y=[phs for x in range(len(st_d))]
        p.segment(x0=st_d, y0=y, x1=et_d, y1=y, line_color="#f2162c", line_width=7)        
        
        #finish plotting green-yellow-red for each phase

    # prepare detector data
    dummydet=eventParse(df,4)    
    for det in sorted(detlst):
        spm_det = parse_more(dummydet,1,[det]) # get down to the particular phase but with all phs events
        spm_det = parse_more(spm_det,0,[82,81]) # get down to on/off events
        
        print("working on detector:",det,"....")
        # get phase on-duration time stamps
        phs +=1 # reuse phs parameter to increment the height
        y=[phs for x in range(len(st_d))]
        st_d, et_d = gen_onoff4plt(spm_det,82,81)
        # prepare and plot On-duration times for this detector
        p.segment(x0=st_d, y0=y, x1=et_d, y1=y, line_color="#391fb8", line_width=7)        
        
        # finish plotting O-duration for each detectors
        
    # setup figure config
    # create ylabel dictionary
    ylbl_ovrr = {}
    lbl = ['phs_'+str(x) for x in sorted(phaselist)]+['det_'+str(x) for x in sorted(detlst)]
    for i,item in enumerate(lbl):
        ylbl_ovrr[i+1] = item
        
    p.xaxis.formatter=DatetimeTickFormatter(days=["%m/%d %H:%M"],months=["%m/%d %H:%M"],hours=["%m/%d %H:%M"])#,minutes=["%m/%d %H:%M"])
    # set the grid interval filling
    p.xgrid.band_fill_color = "grey"
    p.xgrid.band_fill_alpha = 0.05
    p.yaxis.ticker = [x for x in range(1,len(lbl)+1)]
    p.yaxis.major_label_overrides = ylbl_ovrr
    
    show(p)
    

def get15minbin(df):
    """This function generates a column for number coded to the 15 mins period for a dataframe. The dataframe
    needs to be prepared with datetime index and hour/minute columns"""
    df['min_bin']=np.ceil(((df.hour*60+df.minute).values)/15)
    
    return df