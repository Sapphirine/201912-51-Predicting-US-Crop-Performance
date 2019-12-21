# 201912-51-Predicting-US-Crop-Performance
EECS-E6893 final project code repo (project id:201912-51)

### [Live Demo](http://christnp.pythonanywhere.com/#1)

### Repo Structure

src/  
|-- usheatmap/  
|-- mllib/  
|-- website/  
|-- .tmp/

__usheatmap:__ has all of the data preprocessing source code  
__mllib:__ has all of the Spark MLlib source code (notebook)  
__website:__ has all of the visualization source code  
__.tmp:__ has sample data needed by `mod_test.py` to exercise the `usheatmap` data preprocessing source code (unless data is downloaded from the FTP servers using the suppled scripted.)

### Running the data preprocessing code
To run the data preprocessing code, some setup is required. You must setup a Google
account with Google BQ api enabled. You must then generate a Google Auth key (in JSON format)
and point to it in `mod_test.py`. You must also create a dataset and set it
accordingly in `mod_test.py`.


There are example, parsed files, stored in the /src/.tmp/ directory. These files can be ran locally using the mod_test.py script and setting the following parameters to false:

```Python
# mod_test.py
    GET_VH = False
    PARSE_VH = False
    #...
    GET_C5 = False     
    PARSE_C5 = False 
```
The `mod_test.py` script is designed to excercise each of the data preprocessing modules independently. Be sure to disable Google BQ storing if it is not being used OR set the appropriate parameters in `mod_test.py`.