# Variable changes

train_1 → train_data
test_1  → test_data
filePath → file_path
df_short_1 → df_short
results → forecast_results
item_df_short → dates_df_short #Unused variable

# Style

Used linter → pylint, thir can be change to adhere to group standards.

#  Long queries Encapsulation
Create a separate file for long queries such that it is easier to maintain them and share across projects. It also makes the principal code less clutter and more user friendly

# List of regressors Encapsulation
Create a separate file for long list of regressors and loop over it to add all in one go, this also has the advantage of making the code less clutter and it also makes it easier to use the same set of regressors over several protects. 

# Remove unused debugging lines.
There are some lines commented with variants of the df's and variables, this are often used in the development phase but should be removed ones the code is no longer on dev.

# Library placement
Move all imports to top of the file to avoid double imports and give a clear picture of all the necessary libraries to execute the code. 