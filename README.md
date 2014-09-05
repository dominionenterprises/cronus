#cronus
[![Build Status](http://img.shields.io/travis/dominionenterprises/cronus.svg?style=flat)](https://travis-ci.org/dominionenterprises/cronus)
[![Scrutinizer Code Quality](http://img.shields.io/scrutinizer/g/dominionenterprises/cronus.svg?style=flat)](https://scrutinizer-ci.com/g/dominionenterprises/cronus/)
[![Code Coverage](http://img.shields.io/coveralls/dominionenterprises/cronus.svg?style=flat)](https://coveralls.io/r/dominionenterprises/cronus)

[![Latest Stable Version](http://img.shields.io/packagist/v/dominionenterprises/cronus.svg?style=flat)](https://packagist.org/packages/dominionenterprises/cronus)
[![Total Downloads](http://img.shields.io/packagist/dt/dominionenterprises/cronus.svg?style=flat)](https://packagist.org/packages/dominionenterprises/cronus)
[![License](http://img.shields.io/packagist/l/dominionenterprises/cronus.svg?style=flat)](https://packagist.org/packages/dominionenterprises/cronus)

Process registry using MongoDB as a backend.

##Features
 * Automatic process cleaning when:
  * Process not running
  * User-given expire time elapses
  * Process id is reused
 * User-given process count limits
  * Across all hosts
  * Per host
 * Concurrent safety using an optimistic method
 * Resettable expire times

##Simple example

```php
use DominionEnterprises\Cronus\ProcessRegistry;

$mongo = new MongoClient();
$collection = $mongo->selectDB('testing')->selectCollection('processes');

if (!ProcessRegistry::add($collection, 'unique id for this script', 60)) {
    return;
}

//do work that SHOULDN'T be done concurrently
```

In this example the work is only being done by one process at one time despite how many of these scripts start, which is due to a max processes
setting of 1. This is the default and can be changed on a global and/or host basis.

A good setup is a collection of servers with these scripts run from a cron. Since the cron will continue to run the script trying the add()
method, reliability is achieved should one fail (automatically cleaned up) or get stuck (automatically cleaned after 60 minutes).

##Composer & Requirements

To add the library as a local, per-project dependency use [Composer](http://getcomposer.org)! Simply add a dependency on
`dominionenterprises/cronus` to your project's `composer.json` file such as:

```json
{
    "require": {
        "dominionenterprises/cronus": "~1.0"
    }
}
```

In addition to the composer dependencies the project relies on [procfs](http://en.wikipedia.org/wiki/Procfs).

##Documentation

Found in the [source](src/ProcessRegistry.php) itself, take a look!

##Contact

Developers may be contacted at:

 * [Pull Requests](https://github.com/dominionenterprises/cronus/pulls)
 * [Issues](https://github.com/dominionenterprises/cronus/issues)

##Project build

Install and start [mongodb](http://www.mongodb.org).
With a checkout of the code get [Composer](http://getcomposer.org) in your PATH and run:

```sh
./build.php
```
