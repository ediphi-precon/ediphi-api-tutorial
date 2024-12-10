<div style="background-color: white; display: flex; justify-content: center; align-items: center; padding: 10px; border-bottom: 2px solid #6e6e6e;">
<img src="https://cdn.prod.website-files.com/658da2e31815cdd2c27ecf68/658daa80cbeb870d23cdbe87_logo_ediphi_website.svg">
</div>

## API Tutorial

### Purpose

At ediphi we believe all of your preconstruction data should be accessible to you on demand so that you can use it in your other business systems. This repository serves as a tutorial for using our API via external endpoints, where data.ediphi.com can return just about anything within your database.

### Getting Started

First you will need an API key and database number. Reach out to your ediphi contact to receive these and keep them safe. We [bcrpyt](https://en.wikipedia.org/wiki/Bcrypt) our API keys and once we issue them we cannot recover them. They are visible to us only at the time of creation, and then sent to you securely typically by encrypted email. After receiving your API key and database number follow these steps:

1.  Once you receive your API key and database number, copy the .env.example file to a .env file and enter in the `X_API_KEY` and `DATABASE_NO`. Assuming you are on Windows and your terminal is at the root directory of this repository, run:

        copy .env.example .env

2.  Create a virtual env if you wish, but the the requirements are quite lean for this tutorial, so it may not be required. Activate the virtual environment first if you are using one, and then to install requirements run:

        pip install -r requirements.txt

3.  That's it, check out the notebook `tutorial.ipynb` to step through the sample code.

### Additional Information

Refer to the `sample_data.js` file to see samples of the main data elements in ediphi with some comments. This is useful when making requests for data filtering by object properties while using the data pipeline.
