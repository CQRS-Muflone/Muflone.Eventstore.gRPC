# Muflone.Eventstore.gRPC

Muflone repository and event dispatcher for [EventStoreDB](https://eventstore.org) using gRPC (TCP is deprecated since version 20.x).

## Install

`Install-Package Muflone.Eventstore.gRPC`

## Sample usage

Look at [this repo](https://github.com/CQRS-Muflone/CQRS-ES_testing_workshop)

## Sample connection string

    esdb://localhost:2113?tls=false&tlsVerifyCert=false

Watch out for the correct connection port. You must use the HTTP port (default 2113) and not the TCP one anymore (default 1113).

Also, use `tls=false` only for development environment. [Read more on the offical documentation](https://developers.eventstore.com/server/v23.10/security.html#security)
