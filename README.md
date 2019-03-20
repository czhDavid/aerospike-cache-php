Aerospike cache adapters for PHP
===================

[![Latest Stable Version](https://img.shields.io/packagist/v/lmc/aerospike-cache-php.svg)](https://packagist.org/packages/lmc/aerospike-cache-php)
[![Build Status](https://travis-ci.org/lmc/aerospike-cache-php.svg?branch=master)](https://travis-ci.org/lmc/aerospike-cache-php)
[![Coverage Status](https://coveralls.io/repos/github/lmc/aerospike-cache-php/badge.svg?branch=master)](https://coveralls.io/github/lmc/aerospike-cache-php?branch=master)

This component provides a PSR-6 implementation of Aerospike for adding cache to your applications. 

## Installation

```bash
composer require lmc/aerospike-cache
```

## Usage
```php
$aerospike = new \Aerospike(['hosts' => [['addr' => '127.0.0.1', 'port' => 3000]]]);
$aerospikeCache = new AerospikeCache($aerospike, 'aerospkeNamespace');
```

AerospikeCache uses PSR-6 caching interface for manipulation with the data.  
The AerospikeCache also comes bundled with Symfony Cache adapter for DoctrineCache

Creating a new Aerospike instance will immediately connect to the aerospike server,
 which might take a while and it should be in `lazy` abstraction.

## Changelog
For latest changes see [CHANGELOG.md](CHANGELOG.md) file. We follow [Semantic Versioning](https://semver.org/).

## Contributing and development

### Install dependencies

```bash
composer install
```

### Run tests

For each pull-request, unit tests as well as static analysis and codestyle checks must pass.

To run all those checks execute:

```bash
composer all
```
