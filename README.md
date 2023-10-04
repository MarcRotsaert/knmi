# Download and Presenting KNMI data

## Introduction 
retrieve most recent model and measurment data from REST API KNMI open data and present timeseries of both of location Rotterdam Airport. 
Soure model data is the operational Harmonie model.

## Getting Started
### Prerequisite: 
    Runs on Windows
    Python: tested on python 3.9
    API -key: request API keys for model and measurment data from KNMI site.
    Several Gb free space

## Before startup:
### API
    Two API-tokens should be requested at knmi, one for the model data and one for the measurments
    Information can be found at 
    https://developer.dataplatform.knmi.nl/open-data-api
    Add the api keys to callpython.bat
        (KNMI_API_MODEL and KNMI_API_METING

###Installation code
    Get the code from github.
    The default directory for the code is C:\TEMP\knmi

## Startup
    simply run the following code from a cmd-shell
        callpython.bat 
## Result
A png should be created in C:\temp after while. 


## Contribute
TODO: Explain how other users and developers can contribute to make your code better. 