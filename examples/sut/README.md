


## Setting Up the Local Integration Test Environment

This guide explains how to set up the required local tooling and run integration tests against a LocalStack-based AWS environment.

## Prerequisites

Ensure you have the following installed:

- Docker
- Docker Compose
- Node.js and npm
- Python package manager such as `pip`, `pip3`, or `pipx`
- Homebrew, if installing LocalStack on macOS

## Install LocalStack

LocalStack provides a local AWS-compatible environment for development and testing.

For more information, see the [LocalStack GitHub repository](https://github.com/localstack/localstack).

Install the LocalStack CLI:

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




