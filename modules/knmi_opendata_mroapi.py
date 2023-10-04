
"""
Info knmi data plaform voor developers:
 https://developer.dataplatform.knmi.nl/

gdal niet installeren via "pip install GDAL"
 maar apart een wheel downloaden en wheel installeren.
gdal is vervolgens te vinden onder osgeo

vector is een "eigen module"
"""
import shutil
import datetime as dt
import logging
import os
import pprint
import sys
import tarfile
import time

from datetime import datetime, timedelta
from email.errors import NonPrintableDefect
from pathlib import Path

import netCDF4
import numpy as np
import pandas as pd
import requests
import vector
from matplotlib import pyplot
from osgeo import gdal

logging.basicConfig(filename='knmi_opendata.log')
logger = logging.getLogger(__name__)
#logger.setLevel("ERROR")
logger.setLevel("WARNING")

tempdir = 'C:/temp'
api_url = "https://api.dataplatform.knmi.nl/open-data"
api_version = "v1"

#api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImNjOWE2YjM3ZjVhODQwMDZiMWIzZGIzZDRjYzVjODFiIiwiaCI6Im11cm11cjEyOCJ9"  # anonieme key
me_api_maxkeys = "144"
mo_api_maxkeys = "4"
mo_api_key = os.environ.get('KNMI_API_MODEL')
me_api_key = os.environ.get('KNMI_API_METING')


#https://dataplatform.knmi.nl/dataset/actuele10mindataknmistations-2
me_dataset_name = "Actuele10mindataKNMIstations"
me_dataset_version =  "2"
mo_dataset_name = "harmonie_arome_cy40_p5"
mo_dataset_version = "0.2"

def write_data2file(filename, dataset_file_response):
    """
    Schrijf datastream in geheugen naar een bestand. 
    """
    # Write dataset file to disk
    p = Path(filename)
    p.write_bytes(dataset_file_response.content)
    logger.info(f"Successfully downloaded dataset file to {filename}")

def _timestamp(strdate=None):
    if strdate==None:
        #filename_prefix = 
        timestamp = datetime.utcnow().date().strftime("%Y%m%d")
    else:
        timestamp = strdate
    return timestamp

class ApiKnmi():
    """
    class voor interactie met API KNMI.
    Beschrijving Api
    https://developer.dataplatform.knmi.nl/apis 
    """
    def __init__(self,source,strdate=None):
        """
        Input:
            source:string ['model'/'meting']
            strdate: string ["%Y%m%d"], voorbeeld: '20220418'
        """
        self.source=source
        self.timestamp = _timestamp(strdate)
        prefix_name = self.return_parameters()['prefix_name']
        self.start_after_filename_prefix = f"{prefix_name}_{self.timestamp}"
        
    def return_parameters(self,):
        """
        return parameterset behorende bij object  
        Twee opties:
            a) parameterset bij 'meting'
            b) parameterset bij 'model'
        Output: parametersset [dict]  
        """
        if self.source=='model':
            parameters={ 'prefix_name':"harm40_v1_p5",
                'dataset_name' :mo_dataset_name,
                'dataset_version': mo_dataset_version,
                'api_key' : mo_api_key,
                'max_api_keys' : mo_api_maxkeys,
                'exp_ext': '.tar',
                }
        elif self.source=='meting':
            parameters={'prefix_name':"KMDS__OPER_P___10M_OBS_L2",
                'dataset_name' :me_dataset_name,
                'dataset_version': me_dataset_version,
                'api_key' : me_api_key,
                'max_api_keys' : me_api_maxkeys,
                'exp_ext':'.nc',
                }
      
        return parameters

    def _filerequest(self,):
        par=self.return_parameters()
        #dataset_name,dataset_version,api_key,max_api_keys=self.return_parameters()
        list_files_response = requests.get(
            f"{api_url}/{api_version}/datasets/{par['dataset_name']}/versions/{par['dataset_version']}/files",
            headers={"Authorization": par['api_key'] },
            params={"maxKeys": par['max_api_keys'], "startAfterFilename": self.start_after_filename_prefix},
            #params={ "startAfterFilename": start_after_filename_prefix},
            )
        assert(list_files_response.status_code==200), f'_filerequest not resolved, with HTTP error code: {list_files_response.status_code}'
 
        return list_files_response

    def return_filelist(self):
        """
        return uit filelist uit filerequest naar knmi API.
        """
        files_response = self._filerequest()
        list_files_json = files_response.json()
        logger.info(f"List files response:\n{list_files_json}")
        dataset_files = list_files_json.get("files") #print(dataset_files)
        time.sleep(0.5)
       
        return dataset_files

    def _download_lastfile(self):
        filelist = self.return_filelist()
        filename = filelist[-1]['filename'] 
        print('download:'+filename)
        data = self._download_data(filename)

        return filename,data

    def _download_batch(self):
        filelist = self.return_filelist()
        a=0
        while a<len(filelist):
            filename = filelist[a]['filename'] 
            data = self._download_data(filename)
            yield filename,data
            a+=1

    def write_allfiles(self,):
        """
        Schrijf alle databestanden uit self.returnfilelist .
        """
        gen = self._download_batch()
        for filename,data in gen:
            pathfilewrite= os.path.join(tempdir,filename) 
            write_data2file(pathfilewrite,data)
            print (f'wegschrijven: {pathfilewrite} gereed')

    def write_lastfile(self):
        """
        schrijf laatst aangemaakte data weg naar file
        Output: filenaam weggeschreven file
        """
        filename,data = self._download_lastfile()
        pathfilewrite = os.path.join(tempdir,filename)
        write_data2file(pathfilewrite,data)
        print (f'wegschrijven: {pathfilewrite} gereed')

        return pathfilewrite
        
    def _download_data(self,filename):
        """
        Input: filename waar output naar toegeschreven wordt
        Output: 
        """
        #dataset_name,dataset_version,api_key,max_api_keys=self.return_parameters()
        par=self.return_parameters()

        logger.info(f"Retrieve file with name: {filename}")
        endpoint = f"{api_url}/{api_version}/datasets/{par['dataset_name']}/versions/{par['dataset_version']}/files/{filename}/url"
        get_file_response = requests.get(
            endpoint, headers={"Authorization": par['api_key']})
        if get_file_response.status_code != 200:
            print(get_file_response.status_code)
            logger.error("Unable to retrieve download url for file")
            logger.error(get_file_response.text)
            # sys.exit(1)
           
        download_url = get_file_response.json().get("temporaryDownloadUrl")
        try:
            dataset_file_response = requests.get(download_url)
            if dataset_file_response.status_code != 200:
                logger.error("Unable to download file using download URL")
                logger.error(dataset_file_response.text)
                # sys.exit(1)
        except:
            print('Datadownload mislukt: '+download_url)
            dataset_file_response = None

        return dataset_file_response

class K_tar(ApiKnmi):
    """
    class voor interactie met tar file opgehaald via de knmi api.
    Harmonie model data wordt opgehaald.
    def __init__(self,strdate=None):
    """
    def __init__(self,strdate=None):    
        self.source='model'
        super().__init__(self.source,strdate)

    def _extract_gribfile(self,tarfn,iterator,removetar=True):
        """
        generator om gribfiles uit tar files te extraheren
        Input:
        tarfn: filename tar-file [str]
        iterator: tijdstappen [list met [int]]
        Batch Untar grib=bestand uit tar file. 
        """
        tar = tarfile.open(os.path.join(tempdir,tarfn),'r')
        for nr,it in enumerate(iterator):
            gribfn = tar.getmembers()[it].name
            if not os.path.exists(os.path.join(tempdir,gribfn)):
                tar.extract(tar.getmembers()[it],path=tempdir)
            if nr==len(iterator)-1 and removetar:
                tar.close()
                os.remove(os.path.join(tempdir,tarfn))
            yield gribfn

    def _get_gribobj(self,gribfn):
        return gdal.Open(gribfn)

    def _info_gribband(self,gribfn, bandnr):
        grib = self._get_gribobj(gribfn)
        rb = grib.GetRasterBand(bandnr)
        pprint.pprint(rb.GetMetadata_Dict())

    def _get_rasterband(self,gribfn, bandnr):
        grib = self._get_gribobj(gribfn)
        rb = grib.GetRasterBand(bandnr)

        return rb

    def get_meteo(self,tarfn,timesteps,gribcell,removetar=True,removegrib=True):
        """
        generator extraheer meteo informatie uit grib bestand op een gribcell te halen.
        Vervolgens grib bestand verwijderen na gebruik.
        """
        gribgen = self._extract_gribfile(tarfn,timesteps,removetar)    
        mo_laag_uwind=262
        mo_laag_vwind=263
        mo_laag_temp = 261

        for a in timesteps:
            gribfilename=next(gribgen)
            [basetime,outputtime]=gribfilename.split('_')[2:4]
            outputhours=int(outputtime)/100
            timeutc = datetime.strptime(basetime,'%Y%m%d%H%M')+timedelta(0,outputhours*3600,0)
            gribfile = os.path.join(tempdir,gribfilename)
            gobj = self._get_gribobj(gribfile)
            
            temp = gobj.ReadAsArray(gribcell[0], gribcell[1], 1, 1)[mo_laag_temp][0][0]
            uw = gobj.ReadAsArray(gribcell[0], gribcell[1], 1, 1)[mo_laag_uwind][0][0]
            vw = gobj.ReadAsArray(gribcell[0], gribcell[1], 1, 1)[mo_laag_vwind][0][0]
            gobj=None
            cart = vector.Cart(uw,vw,'CF')
            ws= float(cart.vel)
            wdir = float(cart.degn_cart2naut)
            yield timeutc,temp,ws,wdir
            if removegrib==True:
                os.remove(os.path.join(tempdir,gribfilename)) 

class K_nc(ApiKnmi):
    """
    class voor interactie met netcdf file opgehaald via de knmi api.
    Meetdata wordt opgehaald.
    """
    def __init__(self,strdate=None):
        self.source='meting'
        super().__init__(self.source,strdate)

    def _open_nc(self, filename):
        nc = netCDF4.Dataset(filename)

        return nc

    def _close_nc(self,nc):
        nc.close()

    def _extract_nc(self,nc,parameters,stationame):
        # waarden van bepaalde parameter uit netcdf halen
        """
        Input:  nc: netcdf-object
                parameters: parameter namen zoals gedefinieerd in nc.variables  [iterator met strings]
                stationname: naam locatie [string]
        Output: ti: datetime-object 
                vals: meetwaarden horend bij parameters [ list met waarden (float?)  ]
        """
        i = np.where(nc.variables['stationname'][:] == stationame)[0]
        ti = dt.datetime(1950, 1, 1)+dt.timedelta(0,float(nc.variables['time'][0]), 0)
        vals = []
        for par in parameters:
            vals.append(nc.variables[par][i][0][0])

        return ti, *vals

    def print_stationnames(self,ncfilename):
        nc = self._open_nc(ncfilename)
        print(nc.variables['stationname'][:])

    def print_meteopar(self,ncfilename):
        nc = self._open_nc(ncfilename)
        print(nc.variables.keys())

    def get_meteo(self,filelist,stationname):
        tempname = 'tx'
        wrname = 'dd'
        wsname = 'ff'
        outputfile = os.path.join(tempdir,'wind.nc')
        
        tel = 0
        time,temp, winds,windd =[],[],[],[]
        #for a in sorted(filelist,reverse=True):
        for a in sorted(filelist,reverse=True):
            data=self._download_data(a)
            write_data2file(outputfile, data)
            nc = self._open_nc(outputfile)
            ti, tx, ws,wd = self._extract_nc(nc,[tempname,wsname,wrname],stationname)
            self._close_nc(nc)
            time.insert(0,ti)
            temp.insert(0,tx) 
            winds.insert(0,ws)
            windd.insert(0,wd)
            os.remove(outputfile)
            print(outputfile+ ' removed')
            if tel < int(self.return_parameters()['max_api_keys']):
                tel += 1
            else:
                print('max nr.  requests exceeded')
                break

        return time,temp, winds,windd


class Modelrot(K_tar):
    def __init__(self,strdate=None):
        super().__init__(strdate)
        self.source='model'
        #self.gribcell = (160, 173) #Locatie Rotterdam op basis van schatting 
        self.gribcell = (164, 168) #Locatie Rotterdam op basis van schatting 

    def _test_get_data(self,):
        timesteps = range(10)
        tarfn = 'D:/temp/harm40_v1_p5_2022072012.tar'
        gribcell = self.gribcell
        x= self.get_meteo(tarfn,timesteps,gribcell,False)

        return x

    def _get_data_rotterdam(self,tarfn,timesteps,removetar=True,removegrib=True):
        """
        extraheer meteo informatie uit grib bestand thv Rotterdam Airport.
        Vervolgens grib bestand verwijderen na gebruik.
        input:  tarfn,
        """
        gribcell = self.gribcell
        x= self.get_meteo(tarfn,timesteps,gribcell,removetar,removegrib)
        meteomodel = [a for a in x]

        return meteomodel

    def _rotterdam2df(self):
        """
        Haal meest actuele model gegevens Rotterdam airport op.
        """
        filename = self.write_lastfile()
        timesteps = range(49)
        meteomodel = self._get_data_rotterdam(filename,timesteps)
        #meteomodel = [a for a in x]
        df = pd.DataFrame(meteomodel)

        return df

    def plotrotterdam(self,fig=None):
        """
        plot meest actuele meteo model Rotterdam airport (model of meting)
            fig:    [matplotlib figure] 
        """ 

        df = self._rotterdam2df()
        if fig==None:
            fig,[ax_t,ax_v,ax_d] = pyplot.subplots(3,1,figsize=[20,20])
        else:
            ax_t,ax_v,ax_d=fig.axes
        ax_t,ax_v,ax_d=fig.axes
        ax_t = pyplot.subplot(3,1,1)
        pyplot.plot(df[0],df[1],'b',axes=ax_t, label='Harmonie')
        ax_v= pyplot.subplot(3,1,2)
        pyplot.plot(df[0],df[2],'b',axes=ax_v)
        ax_d= pyplot.subplot(3,1,3)
        pyplot.plot(df[0],df[3],'b',axes=ax_d)

        return fig

class Metingrot(K_nc):
    def __init__(self,strdate=None):
        super().__init__(strdate)
        self.source='meting'
        self.stationname='ROTTERDAM THE HAGUE AP'

    def _rotterdam2df(self,):
        filelist  = self.return_filelist()
        filenamelist = [fi['filename'] for fi in filelist]
        #print(filenamelist)
        time,temp, winds,windd = self.get_meteo(filenamelist,self.stationname)
        #x= meteodata = [a for a in x]
        #print(time)
        #print(temp)
        df = pd.DataFrame(list(zip(time,temp, winds,windd)))

        return df

    def plotrotterdam(self,fig=None):
        """
        plot meteo metingen Rotterdam airport (model of meting)
            fig:    [matplotlib figure] 
        """ 
        df = self._rotterdam2df()

        if fig==None:
            fig,[ax_t,ax_v,ax_d] = pyplot.subplots(3,1,figsize=[20,20])
        else:
            ax_t,ax_v,ax_d=fig.axes
        ax_t = pyplot.subplot(3,1,1)
        pyplot.plot(df[0],df[1],'r',axes=ax_t,label='10-min meting')
        ax_v= pyplot.subplot(3,1,2)
        pyplot.plot(df[0],df[2],'r',axes=ax_v)
        ax_d= pyplot.subplot(3,1,3)
        pyplot.plot(df[0],df[3],'r',axes=ax_d)

        return fig

def plotdf(df,fig=None):
    """
    plot df meteo (model of meting)
    input:  df: DataFrame  [pandas dataframe]
                col[1]=temp
                col[2]=windsnelheid
                col[3]=windrichting
            fig:    [matplotlib figure] 
    """
    if fig==None:
        fig,[ax_t,ax_v,ax_d] = pyplot.subplots(3,1,figsize=[20,20])
    else:
        ax_t,ax_v,ax_d=fig.axes
    ax_t,ax_v,ax_d=fig.axes
    ax_t = pyplot.subplot(3,1,1)
    pyplot.plot(df[0],df[1],'b',axes=ax_t, label='Harmonie')
    ax_v= pyplot.subplot(3,1,2)
    pyplot.plot(df[0],df[2],'b',axes=ax_v)
    ax_d= pyplot.subplot(3,1,3)
    pyplot.plot(df[0],df[3],'b',axes=ax_d)

    return fig

def _layoutfig(fig):
    """
    Layout figure uit plotrotterdam
    """
    Bftlims = [0,0.2,1.5,3.3,5.5,8.0,10.8,13.9,17.2,20.8,24.5,28.5,32.6,]

    ax_t = pyplot.subplot(3,1,1)
    ax_t.grid()
    ax_v= pyplot.subplot(3,1,2)
    maxy = ax_v.get_ylim()[1]+5
    ax_v.set_yticks(Bftlims)
    ax_v.set_yticklabels(zip(Bftlims,range(13)))
    ax_v.set_ylim([0,maxy])
    ax_v.set_ylabel('windsnelheid (m/s, Bft)')
    ax_v.grid()  
    ax_d= pyplot.subplot(3,1,3)
    ax_d.set_ylabel('windrichting (0= Noord, 90= Oost)')
    ax_d.set_ylim([0,360])
    ax_d.set_yticks(list(range(0,450,90)))
    ax_d.grid()


def savefigrotterdam(fig):
    fig.savefig(os.path.join(tempdir,f"knmirotterdam_{datetime.now().strftime('%y%m%d_%H')}.png"))

def _get_geotransform(gribobj):
    (upper_left_x, x_size, x_rotation, upper_left_y,
     y_rotation, y_size) = gribobj.GetGeoTransform()
    decode = (upper_left_x, x_size, x_rotation,
              upper_left_y, y_rotation, y_size)

    return decode

class Startup():
    """
    Class om op verschillende manieren scripts/programma te starten. 
    """
    #def __init__(self):
    #    pass
    def automatic(self):
        """start programma automatisch"""
        self.totalprogram()

    def confirmed(self):
        """start programma naar keus"""
        jn=''
        while jn not in ['j','n']:
            jn = input(r'wil je knmi_opendata... starten?kies(j/n)')
            if jn not in ['j','n']:
                print('vul in j (voor ja) of n (voor nee)')
        if jn=='j':
            self.totalprogram()
        else:
            'Programma niet gestart'
            time.sleep(2)
            pass
    def test(self):
        """
        start testprogramma
        """
        self.test()
class Program(Startup):
    def __init__(self):
        pass

    def test(self):
        ncobj = K_nc()
        ncobj.print_stationnames(r'C:/temp\\KMDS__OPER_P___10M_OBS_L2_202208170920.nc')
        xx
        #K_tar().write_allfiles()
        fig,[ax_t,ax_v,ax_d] = pyplot.subplots(3,1,figsize=[20,20])
        for a in range(0,12,6):
            fn = 'harm40_v1_p5_20220816{:02d}.tar'.format(a)
            datarotterdam = Modelrot('20220816').get_data_rotterdam(fn,range(0,49,),False,True)
            print(len(datarotterdam))
            df= pd.DataFrame(datarotterdam)
            print(df)
            plotdf(df,fig)
            _layoutfig(fig)
        fig.savefig('C:/temp/modeltest2.png')
        #return x
        #Metingrot().write_allfiles()
        #c =Modelrot().test_get_data()
        #meteo = [a for a in c] 
        #print(meteo)

        #fig = Modelrot('20220809').plotrotterdam()
        #fig.savefig(os.path.join(tempdir,f"test{datetime.now().strftime('%y%m%d_%H')}_1.png"))
        #print(df3)
        #df4 = Modelrot('20220811').rotterdam2df()
        #print(df4)
        #fig = Modelrot('20220812').plotrotterdam(fig)
        #print(df3)
        #df4 = Modelrot('20220811').rotterdam2df()
        #print(df4)
        
        #fig.savefig(os.path.join(tempdir,f"test{datetime.now().strftime('%y%m%d_%H')}_2.png"))


        #fig = K_nc().plotmeting(df4)
        #df1 = K_tar('220809').rotterdam2df()
        #df2 = K_tar('20220810').rotterdam2df()
        #df3 = K_tar('220811').rotterdam2df()
        #fig = Modelrot().plotrotterdam(df4)
        #plotmeting(df3,fig)
        #fig = Metingrot().plotrotterdam(df3,fig)
        #fig = K_tar().plotrotterdam(df3,fig)
        #_layoutfig(fig)
        #fig.savefig('D:/temp/ggg.png')

        #df2 = self.download_model()
        #fig2 = plotmodel(df2)
        #fig2.savefig('C:/temp/model.png')
        #df1 = self.download_meting()
        #fig1 = K_nc().plotrotterdam(df1)
        #fig1.savefig('C:/temp/meting.png')
        

        if False:
            dfrot_model = K_tar().rotterdam2df()
            print(dfrot_model)
            if False:
                x= K_tar().get_data_rotterdam('harm40_v1_p5_2022081112.tar',range(49))
                meteomodel = [a for a in x]
                df = pd.DataFrame(meteomodel)
        
        if False:
            fl = Api_knmi('meting').return_filelist(dlfr)
            data = Api_knmi('meting')._download_data(fl[-1]['filename'])
            print(fl)
            data = Api_knmi('meting')._download_data(fl[-1]['filename'])
            write_data2file('C:/temp/ggg1.nc',data)
            dlfr = Api_knmi('model')._filerequest()
            print(dlfr)
            fl = Api_knmi('model').return_filelist(dlfr)
            print(fl)
            data = Api_knmi('model')._download_data(fl[-1]['filename'])
            write_data2file('C:/temp/ggg2.tar',data)

    def plot_meting(self,fig=None):
        fig=Metingrot().plotrotterdam(fig)
        return fig

    def plot_model(self,fig=None):
        fig=Modelrot().plotrotterdam(fig)
        return fig

    def totalprogram(self):
        print('start automatic')
        fig = self.plot_model()
        fig = self.plot_meting(fig)
        _layoutfig(fig)
        savefigrotterdam(fig)

        
if __name__ == "__main__":
    Program().automatic()
    #Program().totalprogram()
    #Program().test()
    #x=Program().test()
    #df = Program().test()
    #df1 = Program().download_meting()
    #fig = plotmeting(df1)
    #_layoutfig(fig)

    #model=Program().download_model()