


## Setting up the environment for running integration tests locally.

### Install localstack
https://github.com/localstack/localstack

```
$ brew install localstack/tap/localstack-cli
```

### Install awscli-local 

https://github.com/localstack/awscli-local

awscli-local is a command-line tool, provided by LocalStack, that acts as a thin wrapper around the standard AWS CLI.
It provides the awslocal command, which is used to interact with a local, simulated AWS environment rather than the
actual cloud services.

You may need to install a package manager for python. pip,pip3,pipx, etc.
```
$ pipx install "awscli-local[ver1]"
```

A quick test to see if it is working:
```
$ awslocal sns list-topics
```

#### Install cdklocal
https://github.com/localstack/aws-cdk-local

```
$ npm install -g aws-cdk-local aws-cdk
...
$ cdklocal --version
```

## Running integration tests locally

```aiignore
cd examples/sut
```

```
docker compose up
```

### Deploy services using cdklocal

```aiignore
$ ./deploy_all.sh 
```

### Run integration tests
```aiignore
$ ./runitest.sh
```




