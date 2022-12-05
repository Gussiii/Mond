# Assessment Mondelez

This is the repertory of my submission to the assessment for the position as a ML Engineer.
To run this project you can clone this project and use the [Spark+Jupyter](https://hub.docker.com/r/jupyter/pyspark-notebook) docker image.

```bash
docker pull jupyter/pyspark-notebook
```

To run the container and expose the ports for jupyter and spark web UI. 
First cd to the project folder and run:

```bash
docker run -it --rm -p 8888:8888 -p 4040:4040-v $(PWD):/home/jovyan/study_case jupyter/pyspark-notebook
```

Copy and paste the Jupyter link in your browser or use it to connect to the remote interpreter on VSCode (proffered method).

Note: To open the Spark web UI go to http://127.0.0.1:4040/
# Project Structure
The assessment is divided into 3 parts, you can find the submission on each of the folders.
- Task 1: Forecasting model with PySpark using historical data
- Task 2: Code refactoring
- TasK 3: Proposal for model and utilization for TPM/TPO