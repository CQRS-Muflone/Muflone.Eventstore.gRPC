# Muflone.Eventstore.gRPC

Muflone repository and event dispatcher for [EventStoreDB](https://eventstore.org "Event store's Homepage") using gRPC (TCP is deprecated since version 20.x).

## Install

`Install-Package Muflone.Eventstore.gRPC`

## Sample usage

Look at [this repo](https://github.com/CQRS-Muflone/CQRS-ES_testing_workshop)

## Sample connection string

    esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false

use `tls=false` only for development environment. [Read more on the offical documentation](https://developers.eventstore.com/server/v23.10/security.html#security)
