# BC Parks API -- Usage Guide (MDS Capstone)

## Overview

The BC Parks API provides access to structured data about parks in
British Columbia, including:

-   Parks and protected areas
-   Activities (hiking, camping, etc.)
-   Facilities (washrooms, campgrounds, etc.)
-   Advisories (closures, alerts)

The API is available primarily via **GraphQL** and is accessed through a
government API gateway.

------------------------------------------------------------------------

## Base Endpoint

https://bcparks.api.gov.bc.ca/graphql

------------------------------------------------------------------------

## Authentication

The API uses an API key.

Required header:

x-api-key: YOUR_API_KEY

------------------------------------------------------------------------

## Quick Test (cURL)

curl -X POST https://bcparks.api.gov.bc.ca/graphql\
-H "Content-Type: application/json"\
-H "x-api-key: YOUR_API_KEY"\
-d '{ "query": "{ parks { name description } }" }'

------------------------------------------------------------------------

## GraphQL Playground

https://bcparks.api.gov.bc.ca/graphql

------------------------------------------------------------------------

## Example Query

query { parks { parkId name description location { latitude longitude }
} }

------------------------------------------------------------------------

## Common Issues

1.  Wrong header → must use x-api-key\
2.  Wrong endpoint\
3.  API key restrictions

------------------------------------------------------------------------

## Recommended Architecture

1.  Backend wrapper (Go / Node)
2.  Cache layer (Redis)
3.  Normalize API data
4.  Frontend calls your backend only

------------------------------------------------------------------------

## Notes

-   Docs are limited
-   Use GraphQL playground to explore
-   Build abstraction layer for stability
