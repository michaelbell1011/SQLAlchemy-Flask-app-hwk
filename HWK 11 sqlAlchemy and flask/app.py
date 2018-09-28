# This is app.py to do Step 2 - Climate App of Advanced Data Storage and Retrieval HW
# objective is to create app routes for at least: precipitation levels, station info, and temp. observations ("tobs")

####################################################
# dependencies as in climate_MB.ipynb
####################################################
import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

####################################################
# set up database connection
####################################################
# In the line below, I added connect_args={'check_same_thread': False} to resolve exception: "SQLite objects created in a thread can only be used in that same thread"
engine = create_engine("sqlite:///Resources/hawaii.sqlite", connect_args={'check_same_thread': False})
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)

####################################################
# variables
####################################################
# Query last year's worth of data from database
# This will store last recorded date from Measurement table
latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

# Next need to unpack date components from above and save as strings
lat_dat = latest_date[0]
split_lat_dat = lat_dat.split("-")
year_val = int(split_lat_dat[0])
month_val = int(split_lat_dat[1])
day_val = int(split_lat_dat[2])

# determine date 1 year prior to latest date determined above
previous_year_date = dt.date(year_val, month_val, day_val) - dt.timedelta(days=365)



# PRECIPITATION QUERY
# Need to create a dictionary of precipitation amounts (averaged across stations) by date 
# where Measurement.date is the key and func.avg(Measurement.prcp) is the value.

# SQL select parameters:
sel_prcp = [Measurement.date, func.avg(Measurement.prcp)]

# query:
last_year_prcp_by_date_results = session.query(*sel_prcp).\
    filter(Measurement.date > previous_year_date).\
    group_by(Measurement.date).all()

# Create a stations dictionary from the list of tuples above
last_year_prcp_by_date_dict = dict(last_year_prcp_by_date_results)
# Will use last_year_prcp_by_date_dict in the route called /api/v1.0/precipitation below



# STATIONS QUERY
# Need to create a dictionary of stations where Station.station is the key and Station.name is the value.

# Query columns station and name from Station table. 
# This will output a list of tuples as station_results
station_results = session.query(Station.station, Station.name).all()
# Create a stations dictionary from the  list of tuples above
stations_dict = dict(station_results)
# Will use stations_dict in the route called /api/v1.0/stations below



# TEMPERATURE QUERY
# Need to create a dictionary of temperature observations (tobs)
# from the past year of data
# from the most active station
# where Measurement.date is the key and Measurement.tobs is the value.

# Station with most tobs
most_active_id = session.query(Measurement.station).\
    group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc()).first()

# Query columns date and tobs in Measurement table. 
# This will output a list of tuples as most_active_station_date_tobs_results
most_active_station_date_tobs_results = session.query(Measurement.date, Measurement.tobs).\
    filter(Measurement.date > previous_year_date).\
    filter(Measurement.station == most_active_id[0]).all()

# Create a most active station tobs dictionary from the list of tuples above
most_active_station_tobs_dict = dict(most_active_station_date_tobs_results)
# Will use stations_dict in the route called /api/v1.0/tobs below



from flask import Flask, jsonify

##################################################
# Flask Setup
##################################################
app = Flask(__name__)


##################################################
# Flask Routes
##################################################
# Below is main page
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App API!<br/>Please format date reqeusts as 'YYYY-MM-DD'."
    )

@app.route("/api/v1.0/precipitation")
def preciptation():
    """Return the last_year_prcp_by_date_dict as json"""
    return jsonify(last_year_prcp_by_date_dict)

@app.route("/api/v1.0/stations")
def stations():
    """Return the stations_dict as json"""
    return jsonify(stations_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return the most_active_station_tobs_dict as json"""
    return jsonify(most_active_station_tobs_dict)

@app.route("/api/v1.0/<start>")
def calc_temps_by_start(start):
    #This query should return a list of a tuple with first value as TMIN, second value as TAVG, and third value as TMAX
    temps_by_start_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).all()
    # tmin will be first element inside of the tuple so index[0] will get tuple and another index[0] will get tmin value
    # tavg will be first element inside of the tuple so index[0] will get tuple and another index[1] will get tavg value
    # tmax will be first element inside of the tuple so index[0] will get tuple and another index[2] will get tmax value
    tmin = temps_by_start_results[0][0]
    tavg = temps_by_start_results[0][1]
    tmax = temps_by_start_results[0][2]
    # Create a dictionary where keys will be string names TMIN, TAVG, and TMAX and value pairs will be tmin, tavg, and tmax
    temps_by_start_dict = {'TMIN': tmin, 'TAVG': tavg, 'TMAX': tmax}
    return jsonify(temps_by_start_dict)

@app.route("/api/v1.0/<start>/<end>")
def calc_temps_by_start_end(start, end):
    #This query should return a list of a tuple with first value as TMIN, second value as TAVG, and third value as TMAX
    temps_by_start_end_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    # tmin will be first element inside of the tuple so index[0] will get tuple and another index[0] will get tmin value
    # tavg will be first element inside of the tuple so index[0] will get tuple and another index[1] will get tavg value
    # tmax will be first element inside of the tuple so index[0] will get tuple and another index[2] will get tmax value
    tmin = temps_by_start_end_results[0][0]
    tavg = temps_by_start_end_results[0][1]
    tmax = temps_by_start_end_results[0][2]
    # Create a dictionary where keys will be string names TMIN, TAVG, and TMAX and value pairs will be tmin, tavg, and tmax
    temps_by_start_end_dict = {'TMIN': tmin, 'TAVG': tavg, 'TMAX': tmax}
    return jsonify(temps_by_start_end_dict)


if __name__ == "__main__":
    app.run(debug=True)