import matplotlib.pyplot as plt
import numpy as np
import json
import pprint

from datetime import datetime,date,timedelta
import os
import sys

from netCDF4 import Dataset

from usheatmap.CountryBoxes import CountryBoxes
# from UshmUtils import UshmUtils
from usheatmap.UshmUtils import UshmUtils


class UshmDataParser():
    """
    docstring description
    """
    def __init__(self,project_base='',country='USA'):
        
        self.rpath = os.path.dirname(os.path.realpath(__file__))
        # if project path not provided, make temp dir where this file resides
        if not project_base:
            project_path = os.path.join(self.rpath,".tmp")
            if not os.path.exists(project_path):
                os.makedirs(project_path)
            print("WARNING: saving FTP data to \'{}\'".format(project_path))
            self.project_base = project_path
        else:
            self.project_base = project_base
        
        self.country = country
        
        self.utils = UshmUtils()
        


    def boundCoords (self,coord_span, coord_step, coord_bounds,res=5):
        '''
        Takes a range/span of float values (coords) separated at a specific float
        step/separation, returning a new range/span and start/stop indices for
        the values that are with the specified bounds.
        Input:
            coord_span:     list of floats representing the initial range of values 
                            (usually a two element list [min,max])
            coord_step:     float representing the step size between values
            coord_bounds:   two-element list of floats representing the bounds
        Output:
            new_idx:        two-element list representing the new start/stop indices
            new_span:       two-element list representing the new range/span after
                            coord_span has been bounded.
            new_coords:     coordinate list after bounds applied (len(arange(new_span,coord_step)))
        '''
        try:
            coords = np.arange(coord_span[0],coord_span[-1]-coord_step,coord_step)
        except Exception as e:
            print(e)
        
        # print("len(coords): {}".format(len(coords)))
        # print("coords: ({},{})".format(coord_span[0],coord_span[1]))
        # print("step: {}".format(coord_step))

        new_coords = []
        idx = []
        for i,coord in enumerate(coords):
            if coord_bounds[0] <= coord <= coord_bounds[1]:
                new_coords.append(np.around(coord,res))
                idx.append(i)
        new_span = [new_coords[0],new_coords[-1]]
        new_idx = [idx[0],idx[-1]]
        # need to remove the last element of new_coords
        return new_idx,new_span,new_coords[:-1 or None]

    def getDateCmip(self,c5_date,debug=0):
        """
        docstring desription
        """
        day=0
        for i,tslice in enumerate(c5_date):
            day += 1
            if i%7 == 0 and i !=0: 
                # uncomment to use YYYY-week#
                # c5_date[i] = datetime.strptime(c5_date[i], "%Y-%m-d").date().strftime('%Y-%W')
                pass
            else:
                # mark non-week dates for removal
                c5_date[i] = "remove"
        # update the date object
        c5_date = [value for value in c5_date if value != "remove"]
        if debug > 2:
            print("Dates for CMIP-5: \n{}".format(c5_date))
        return c5_date

    def getBoundedCmip(self,c5_obj,height,width,fill=-999,lat=[-90,90],lon=[-180,180],debug=0):
        """
        docstring desription
        """
         # indices for bounding
        y0,y1 = lat[0], lat[1]
        x0,x1 = lon[0], lon[1]
        
        c5_new = np.ma.empty((52,height,width),dtype=np.float32)
        c5_tmp = np.ma.empty((1,height,width),dtype=np.float32)
        week_arr = np.ma.empty((7,height,width),dtype=np.float32)
        mean_arr = np.ma.empty((1,height,width),dtype=np.float32)
        tmp = np.ma.empty((height,width),dtype=np.float32)

        week = 0
        day = 0.0
        week_list = []
        mean_list =[]
        for i,tslice in enumerate(c5_obj):
            # bounded dataset for time i (mask removed and masked values set to 0.0)
            tmp = tslice[y0:y1,x0:x1]#.filled(np.nan) # can add any number to .filled(#)
            mask = tmp.mask
            fill = tmp.fill_value

            # ref: https://stackoverflow.com/questions/34357617/append-2d-array-to-3d-array-extending-third-dimension
            week_list.append(tmp)
            day += 1
            if i%7 == 0 and i !=0: 
                week += 1
                # print("day: {0}, week: {1}".format(i,week))
                # taking mean over 7-days to get weekly resolution
                week_arr = np.ma.dstack(week_list) # np.dstack() makes 3rd axis the stack axis, not the 1st axis
                # mean_arr = np.nanmean(week_arr,axis=2) # np.dstack() makes 3rd axis the stack axis, not the 1st axis
                mean_arr = np.ma.mean(week_arr,axis=2) 
                # print(mean_arr.mask)
                mean_list.append(mean_arr)

                c5_tmp = np.ma.concatenate((c5_tmp,np.ma.expand_dims(mean_arr,axis=0)))

                # reset local vars
                week_arr = np.ma.empty((7,height,width),dtype=np.float32)
                mean_arr = np.ma.empty((1,height,width),dtype=np.float32)
                day = 0.0
                week_list = []

        # store list of weekly averages into a numpy array, and reshape (time,lat,lon)
        c5_new = np.ma.dstack(mean_list)
        c5_new = np.moveaxis(c5_new,2,0) # dstack makes 3rd axis the time axis, so swap
        # apply mask
        # ref: https://stackoverflow.com/questions/29165820/apply-mask-array-2d-to-3d
        # mask_new = c5_new*mask[np.newaxis,:,:] # use broadcasting to create a new 3D mask
        # c5_new = np.ma.array(c5_new,mask=mask_new,fill_value=fill)

        return c5_new 

    def convertLonTo180(self):
        """
        docstring desription
        """
        return True
    def convertLatTo90(self):
        """
        docstring desription
        """
        return True
############################# PARSE VH DATA ####################################
    def parseVH(self,prod_path,debug=0):
        """ Documentation for parseVH()

        Function to parse vegetation health data (*.VH.nc) stored in netcdf file
        format. This has only been tested with data found on NOAA STAR ftp server 
        (ftp.star.nesdis.noaa.gov).
        
        Inputs:
        :param: 
        """

        # vh_dir = 'data/ftp.star.nesdis.noaa.gov-static/'
        # vh_file = 'VHP.G04.C07.NN.P2006001.VH.nc'
        # vh_path = os.path.join(self.project_base,vh_dir,vh_file)

        vh = Dataset(prod_path, mode='r') # using netCDF4 to load .nc file
        # TODO: can this be loaded from FTP server directly?

        # attributes
        vh_product     = getattr(vh,'PRODUCT_NAME')
        vh_year        = getattr(vh,'YEAR')
        vh_week        = getattr(vh,'PERIOD_OF_YEAR')
        try:
            vh_y_lat_range = [getattr(vh,'geospatial_lat_min'), 
                                getattr(vh,'geospatial_lat_max')]
            vh_x_lon_range = [getattr(vh,'geospatial_lon_min'), 
                                getattr(vh,'geospatial_lon_max')]
        except Exception as e:
            print("\nLat/Lon attributes have changed. Error: {}\n".format(e))
            print(">>> Attempting the old format now...\n")
            vh_y_lat_range = [getattr(vh,'START_LATITUDE_RANGE'), 
                                getattr(vh,'END_LATITUDE_RANGE')]
            vh_x_lon_range = [getattr(vh,'START_LONGITUDE_RANGE'), 
                                getattr(vh,'END_LONGITUDE_RANGE')]
            print(">>> success!")
        # dimensions
        vh_y_height    = vh.dimensions['HEIGHT'].size
        vh_x_width     = vh.dimensions['WIDTH'].size
        # variables
        vh_vci_attr = vh['VCI']
        vh_vci      = vh['VCI'][:]
        vh_tci_attr = vh['TCI']
        vh_tci      = vh['TCI'][:]
        vh_vhi_attr = vh['VHI']
        vh_vhi      = vh['VHI'][:]
        #variable attributes
        vh_vci_prod_name = vh_vci_attr.long_name
        vh_tci_prod_name = vh_tci_attr.long_name
        vh_vhi_prod_name = vh_vhi_attr.long_name
        vh_vci_units = vh_vci_attr.units
        vh_tci_units = vh_tci_attr.units
        vh_vhi_units = vh_vhi_attr.units
        vh_vci_fill = np.float(vh_vci_attr._FillValue)    # int16 not JSON serializable
        vh_tci_fill = np.float(vh_tci_attr._FillValue)    # int16 not JSON serializable
        vh_vhi_fill = np.float(vh_vhi_attr._FillValue)    # int16 not JSON serializable

        # vh_x_lon_step = (vh_x_lon_range[1]-vh_x_lon_range[0])/vh_x_width
        vh_x_lon_step = (abs(vh_x_lon_range[1])+abs(vh_x_lon_range[0]))/vh_x_width
        # vh_y_lat_step = (vh_y_lat_range[1]-vh_y_lat_range[0])/vh_y_height
        vh_y_lat_step = (abs(vh_y_lat_range[1])+abs(vh_y_lat_range[0]))/vh_y_height

        console = "[{}] Parsing \'{}\' data...".format(self.utils.timestamp(),vh_product)     # Console output string
        if debug > 1:
            # VH
            console += "\nVH Data Specs \n"
            console += "  vh_product: {}\n".format(vh_product)
            console += "  vh_year: {}\n".format(vh_year)
            console += "  vh_week: {}\n".format(vh_week)
            console += "  vh_x_lon_range: {}\n".format(vh_x_lon_range)
            console += "  vh_y_lat_range: {}\n".format(vh_y_lat_range)
            console += "  vh_x_width: {}\n".format(vh_x_width)
            console += "  vh_y_height: {}\n".format(vh_y_height)
            console += "  x_lon_step: {}\n".format(vh_x_lon_step)
            console += "  y_lat_step: {}\n".format(vh_y_lat_step)
            console += "  vci.scale_factor: {}\n".format(vh_vci_attr.scale_factor)
            console += "  vci.add_offset: {}\n".format(vh_vci_attr.add_offset)
            console += "  tci.scale_factor: {}\n".format(vh_tci_attr.scale_factor)
            console += "  tci.add_offset: {}\n".format(vh_tci_attr.add_offset)
            console += "  vhi.scale_factor: {}\n".format(vh_vhi_attr.scale_factor)
            console += "  vhi.add_offset: {}\n".format(vh_vhi_attr.add_offset)
        print(console)

        ######################## convert the data ##############################
        vh_date = []
        # res = datetime.strptime("2018 W30 w1", "%Y %W w%w")
        d = "{}-{}-0".format(vh_year,vh_week) # set for YYYY-MM-Sunday
        vh_date.append(datetime.strptime(d, "%Y-%W-%w").date().strftime('%Y-%m-%d %H:%M:%S'))

        # uncomment to use YYYY-week#
        # d = "{}-{}".format(vh_year,vh_week) # set for YYYY-MM-Sunday
        # vh_date.append(d)

        ######################### TODO: MAKE THIS A GLOBAL #####################
        # tmp_boxes = json.load(open('../../scratch/country_boxes.json'))
        # us_bounds = tmp_boxes['USA']
        us_bounds = CountryBoxes().getBox('USA')
        lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
        lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon
        ############################################################################
        
        # new longitude and width indices
         # Longitude = (-180.0 + (i+0.5)* dLon) (i: counts from 0 to 9999) 
        # i = (lon + 180.0)/dLon - 0.5 
        # vh_x_lon_idx = [int((lon_bounds[0] + 180.0)/vh_x_lon_step - 0.5),
        #                 int((lon_bounds[1] + 180.0)/vh_x_lon_step - 0.5)]
        # vh_x_lon_range = lon_bounds
        # vh_x_lon = np.arange(lon_bounds[0],lon_bounds[1]-vh_x_lon_step,vh_x_lon_step).tolist()

        vh_x_lon_idx, vh_x_lon_range,vh_x_lon = self.boundCoords(vh_x_lon_range,vh_x_lon_step,lon_bounds)
        vh_x_width = abs(vh_x_lon_idx[1] - vh_x_lon_idx[0])

        # new latitude and height indices
        # Latitude = (75.024 - (j+0.5) *dLat) (j: counts from 0 to 3615)
        # j = (75.024-lat)/dLat - 0.5
        # vh_y_lat_idx = [int((75.024-lat_bounds[1] )/vh_y_lat_step - 0.5),
        #                 int((75.024-lat_bounds[0])/vh_y_lat_step - 0.5)]
        # vh_y_lat_range = lat_bounds
        # vh_y_lat = np.arange(lat_bounds[0],lat_bounds[1],vh_y_lat_step).tolist()   

        vh_y_lat_idx, vh_y_lat_range,vh_y_lat = self.boundCoords(vh_y_lat_range,vh_y_lat_step,lat_bounds)
        vh_y_height = abs(vh_y_lat_idx[1] - vh_y_lat_idx[0])
        
        ############## apply bounds and convert VH to weekly data ##############
        vh_vci_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
        vh_tci_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
        vh_vhi_new = np.ma.empty((1,vh_y_height,vh_x_width),dtype=np.float32)
        # indices for bounding
        y0,y1 = vh_y_lat_idx[0], vh_y_lat_idx[1]
        x0,x1 = vh_x_lon_idx[0], vh_x_lon_idx[1]
        # new matrices using slicing
        vh_vci_new[:] = vh_vci[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
        vh_tci_new[:] = vh_tci[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
        vh_vhi_new[:] = vh_vhi[y0:y1,x0:x1]#.filled() # can add any number to .filled(#)
        ########################## FORMAT OUTPUT ###############################
        vh_json = {}

        vh_json['type'] = vh_product
        vh_json['attr'] = []
        vh_json['prod']  = []

        vh_json['attr'].append({
            # 'year':         vh_date[0].year,
            'date':         vh_date,
            'time_dim':     len(vh_date),
            'lon':          vh_x_lon,
            'lon_start':    vh_x_lon_range[0],
            'lon_end':      vh_x_lon_range[1],
            'lon_step':     vh_x_lon_step,
            'lon_dims':     vh_x_width,
            'lon_units':    [],
            'lat':          vh_y_lat,
            'lat_start':    vh_y_lat_range[0],
            'lat_end':      vh_y_lat_range[1],
            'lat_step':     vh_y_lat_step,
            'lat_dims':     vh_y_height, 
            'lat_units':    []
        })
        vh_json['prod'].append({
            'type':     'VCI', #vh_vci_prod_name, # they decided to change the naming scheme...
            'units':    vh_vci_units,
            'mask':     vh_vci_new.mask.tolist(),
            'fill':     vh_vci_fill,
            'data':     vh_vci_new.tolist() # to get np.ndarray do np.array(json['data'])
        })
        vh_json['prod'].append({
            'type':     'TCI', #vh_tci_prod_name,
            'units':    vh_tci_units,
            'mask':     vh_tci_new.mask.tolist(),
            'fill':     vh_tci_fill,
            'data':     vh_tci_new.tolist() # to get np.ndarray do np.array(json['data'])
        })
        vh_json['prod'].append({
            'type':     'VHI', #vh_vhi_prod_name,
            'units':    vh_vhi_units,
            'mask':     vh_vhi_new.mask.tolist(),
            'fill':     vh_vhi_fill,
            'data':     vh_vhi_new.tolist() # to get np.ndarray do np.array(json['data'])
        })

        # vegetation condition index (vci) = % change from previous year [this is "a proxy for moisture condition"]
        # temperature condition index (tci) = % change from previous year [this is a "proxy for thermal condition"]

        if debug > 1:
            pprint.pprint(vh_json)

        return vh_json
    

    def parseCmip(self,prod_path,product='pr',debug=0):
        """ docstring description

        """
        # load the product dataset
        c5 = Dataset(prod_path, mode='r') # using netCDF4 to load .nc file
        # Seems attribute names are not standard... this method searchs if it
        # does not exists as expected
        try:
            c5_model_id = getattr(c5,'model_id')
        except AttributeError as e:
            model_srch = [s for s in vars(c5) if 'model_id' in s][0]
            c5_model_id = getattr(c5,model_srch,'unknown')
            if debug>1:
                print("getattr(c5,'model_id'): {}".format(e))
        # dimensions
        c5_bands_dim    = c5.dimensions['bnds'].size
        c5_y_height     = c5.dimensions['lat'].size
        c5_x_width      = c5.dimensions['lon'].size
        c5_time_dim     = c5.dimensions['time'].size
        # variables
        c5_lon          = c5['lon'][:]      # units: degrees_east
        c5_x_lon_units    = c5['lon'].units
        # c5_lon_bands    = c5['lon_bnds']
        c5_lat          = c5['lat'][:]      # units: degrees_north
        c5_y_lat_units    = c5['lat'].units
        # c5_lat_bands    = c5['lat_bnds']
        c5_time         = c5['time'][:]        # units: days since 1900-01-01
        c5_time_units   = c5['time'].units
        # c5_time_bnds    = c5['time_bnds']
        c5_prod         = c5[product]
        c5_prod_units   = c5[product].units
        c5_prod_fill    = np.float(c5[product]._FillValue)
        # c5_prod_name    = c5[product].long_name   #NOTE: removed because it is inconsistent
        c5_prod_name    = product
        
        ######################## convert the data ##############################
        #   - to proper coordinate range (-180 to 180, -90 to 90)
        #   - time from days since 1900.01.01 to year/week
        # ref: https://stackoverflow.com/questions/46962288/change-longitude-from-180-to-180-to-0-to-360
        # convert coordinates
        # TODO: implement these hepers
        self.convertLonTo180()
        self.convertLatTo90()
        c5_x_lon_new = ((np.asarray(c5_lon[:]) - 180) % 360) - 180
        c5_x_lon_range = [c5_x_lon_new[0],c5_x_lon_new[-1]]
        c5_y_lat_new = ((np.asarray(c5_lat[:]) - 90) % 180) - 90
        c5_y_lat_range = [c5_y_lat_new[0],c5_y_lat_new[-1]]
        # update step
        # TODO: abs() is required because step < 0 does not seem to work in boundCoords()
        c5_x_lon_step = (abs(c5_x_lon_range[0])+abs(c5_x_lon_range[1]))/c5_x_width
        c5_y_lat_step = (abs(c5_y_lat_range[0])+abs(c5_y_lat_range[1]))/c5_y_height
        # convert days since 1900.01.01 to datetime
        # ref: https://stackoverflow.com/questions/38691545/python-convert-days-since-1990-to-datetime-object
        c5_date = []
        for i in range(0,c5_time_dim):
            start = date(1900,1,1)      # This is the "days since" part
            delta = timedelta(c5_time[i])     # Create a time delta object from the number of days
            c5_date.append((start + delta).strftime('%Y-%m-%d %H:%M:%S'))#.strftime('%Y-%m-%d'))      # Add the specified number of days to 1990

        console = "[{}] Parsing CMIP-5 {} \'{}\' data...".format(self.utils.timestamp(),c5_model_id,c5_prod_name)     # Console output string
        if debug > 1:
            # CMIP5-LOCA
            console += "\nCMIP5-LOCA Specs \n"
            console += "c5_model_id: {}\n".format(c5_model_id)
            console += "c5_bands_dim: {}\n".format(c5_bands_dim)
            console += "c5_y_height: {}\n".format(c5_y_height)
            console += "c5_x_width: {}\n".format(c5_x_width)
            console += "c5_time_dim:  {}\n".format(c5_time_dim)
        print(console)

        ######################### bound the data ###############################
        # https://boundingbox.klokantech.com/
        #   - USA:  [[[-125.2489197254,25.0753572133],[-66.6447598244,25.0753572133],
        #               [-66.6447598244,49.2604253419],[-125.2489197254,49.2604253419],
        #               [-125.2489197254,25.0753572133]]]
        # https://gist.github.com/graydon/11198540
        #   - country boxes in format: [SW.lat, SW.lon, NE.lat, NE.lon]
        # tmp_boxes = json.load(open('country_boxes.json'))
        # us_bounds = tmp_boxes['USA']
        us_bounds = CountryBoxes().getBox(self.country)
        lon_bounds = [us_bounds[1],us_bounds[3]] # lat.lower = SW.lat, lat.upper = NE.lat
        lat_bounds = [us_bounds[0],us_bounds[2]] # lon.lower = SW.lon, lon.upper = NE.lon

        # new longitude and width indices
        c5_x_lon_idx,c5_x_lon_range,c5_x_lon = self.boundCoords(c5_x_lon_range,c5_x_lon_step,lon_bounds)
        c5_x_width = abs(c5_x_lon_idx[1] - c5_x_lon_idx[0])

        # new latitude and height indices
        c5_y_lat_idx, c5_y_lat_range,c5_y_lat = self.boundCoords(c5_y_lat_range,c5_y_lat_step,lat_bounds)
        c5_y_height = abs(c5_y_lat_idx[1] - c5_y_lat_idx[0])

        ############## apply bounds and convert C5 to weekly data ##############
        c5_prod_new = np.empty((c5_y_height,c5_x_width,52))
        # use helper function to get CMIP date and bounded data
        c5_date = self.getDateCmip(c5_date,debug=debug)
        c5_prod_new = self.getBoundedCmip(c5_prod,c5_y_height,c5_x_width,
                                            lat=c5_y_lat_idx,lon=c5_x_lon_idx,debug=debug)

        # getting memory error when allocating numpy arrays:
        # https://stackoverflow.com/questions/57507832/unable-to-allocate-array-with-shape-and-data-type
        ########################################################################
        c5_json = {}

        c5_json['type']     = c5_model_id # TODO: make this cmip-5 + model
        c5_json['attr']     = []    # global attributes
        c5_json['prod']       = []    # pr specific

        c5_json['attr'].append({
            # 'year':         c5_date[0].year,
            'date':         c5_date,
            'time_dim':     len(c5_date),
            'lon':          c5_x_lon,
            'lon_start':    c5_x_lon_range[0],
            'lon_end':      c5_x_lon_range[1],
            'lon_step':     c5_x_lon_step,
            'lon_dims':     c5_x_width,
            'lon_units':    c5_x_lon_units,
            'lat':          c5_y_lat,
            'lat_start':    c5_y_lat_range[0],
            'lat_end':      c5_y_lat_range[1],
            'lat_step':     c5_y_lat_step,
            'lat_dims':     c5_y_height,
            'lat_units':    c5_y_lat_units
        })

        c5_json['prod'].append({
            'type':     c5_prod_name,
            'units':    c5_prod_units,
            'fill':     c5_prod_new.fill_value,
            'mask':     c5_prod_new.mask.tolist(),
            'data':     c5_prod_new.tolist() # to get np.ndarray do np.array(json['data'])
        })

        # precipitation, kg m-2 s-1 (converted to mm/day in 'Subset Request' interface)
        # minimum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        # maximum surface air temperature, °K (converted to °C in 'Subset Request' interface)
        if debug > 1:
            # pprint.pprint(vh_json)
            pprint.pprint(c5_json)
        
        return c5_json

if __name__ == "__main__":
 
    pass
    