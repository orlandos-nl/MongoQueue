# ``MongoQueue``

A MongoDB-based Job Queue API for Swift applications.

## Overview

This library provides a framework for storing and processing a queue of jobs in MongoDB. It uses [MongoKitten](https://github.com/orlandos-nl/MongoKitten) as a driver, using a `MonogKitten.MongoCollection` of your choosing for storing these jobs.

## Topics

- ``MongoQueue``

### Job Types

- ``ScheduledTask``
- ``RecurringTask``
