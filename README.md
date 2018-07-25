# Local setup 

## Set up Python environment for bot

due to pip not working, install pip after creating venv: https://askubuntu.com/questions/488529/pyvenv-3-4-error-returned-non-zero-exit-status-1
```
cd <spread-dir>
python3.6 -m venv venv --without-pip
source venv/bin/activate
curl https://bootstrap.pypa.io/get-pip.py | python
deactivate
source venv/bin/activate
./operations/update_dependencies.sh
```

## Serverless
Make sure serverless is installed globally:
`npm install -g serverless`

Install serverless plugins in repo: 
`npm install --save serverless-python-requirements`

## Set up Python environment for ipython notebooks
Due to ipython requiring different dependencies from the app, a separate environment is configured via an environment.yml
file. 

Install Anaconda.

[Create the "arbitrage" environment from the environment.yml file](https://conda.io/docs/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)

Start the Anaconda application and select the "arbitrage" environment. Run any notebook in "notebooks".

## Set up Pycharm
Set the python binary in the virtual environment [as the project interpreter](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html#local-interpreter). 

[Set Pytest as the test runner](https://www.jetbrains.com/help/pycharm/run-debug-configuration-nosetests.html) so 
that tests can be run from Pycharm. 


## Set environment variables:
Copy `.app_bash_profile.sample` to `.app_bash_profile`. 
 
See src/properties/ab_properties.py for an understanding of the environment variables used. Look at the 
"environment.variables" property of the lambda configurations in terraform/lambda.tf for an understanding of the 
variables used in production. Parameter store is used to populate other env variables not seen in the
terraform configuration. 

```
EXPORT LIMITEDUSER_PG_PASSWORD=<local-limiteduser-password>
EXPORT PROD_PG_PASSWORD=<rds-host-password>
EXPORT RDS_HOST=<rds-host>

EXPORT USE_PROD_DB=False
EXPORT USE_TEST_DATA=False
EXPORT WRITE_TO_S3=False

```
## Database
install postgres:
`brew install postgresql`

Login via superuser and create a role, <username>
```
CREATE ROLE <username> WITH PASSWORD <password> 
ALTER ROLE <username> CREATEDB; 
```
Choose a different password from your superuser password. The reason you create a new role is so that your superuser 
credentials are less likely to be compromised. 

See [here](https://www.codementor.io/engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb) for more 
details.

Then create the market_data database:
```
createdb <username>
psql -U <username>
create database market_data;
```

# AWS setup


## Package AWS lambda and deploy Terraform infrastructure
`deploy_lambda.sh` - runs `terraform apply` as part of the script. `deploy_lambda.sh stage` deploys any "dev" versions
of the lambdas. `deploy_lambda.sh prod` deploys the prod versions. 

## Add exchange API secrets to AWS Parameter Store
Create an API key for each exchange that is being arbitraged. Record the key and secret. 

Go to the [parameter store console](https://us-west-1.console.aws.amazon.com/systems-manager/parameters/) and add a new 
parameter that is a dictionary consisting of the credentials for exchange.

{
    <exchange1_name>: {"key": "<exchange1_key>", "secret": "<exchange1_secret>"}
    <exchange2_name>: {"key": "<exchange2_key>", "secret": "<exchange2_secret>"}
}

# Distribution

```bash
# bump "version" field in setup.py
./operations/package_wheel.sh
```

# Set up remote IDE
https://medium.com/@erikhallstrm/work-remotely-with-pycharm-tensorflow-and-ssh-c60564be862d
 

## Install python 3 

## Set up files
`./deploy_lambda staging`
scp -i ~/.aws/bastion_host.pem ~/.aws/credentials ec2-user@13.56.26.116:~/
scp -i ~/.aws/bastion_host.pem /dist/staging/arbitrage_bot.zip ec2-user@13.56.26.116:~/
ssh -i ~/.aws/bastion_host.pem ec2-user@13.56.26.116
> ec2-user@50.18.198.4 cd ~
> ec2-user@50.18.198.4 mkdir arbitrage_bot
> ec2-user@50.18.198.4 mv arbitrage_bot.zip arbitrage_bot
> ec2-user@50.18.198.4 mv cd arbitrage_bot && unzip arbitrage_bot.zip

set up PyCharm, exclude necessary directories, map `[local]/arbitrage_bot` to
`<remote>/arbitrage_bot`

Add environment variables to PyCharm run configuration.
 
[Boto](http://boto3.readthedocs.io/en/latest/guide/configuration.html#environment-variables)
* AWS_SECRET_ACCESS_KEY
* AWS_ACCESS_KEY_ID
* AWS_DEFAULT_REGION: us-west-1

All of the env variables that the arbitrage_executer lambda uses. See the .tf file. 

