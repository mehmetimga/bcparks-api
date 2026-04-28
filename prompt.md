# BC Parks API -- Usage Guide (MDS Capstone)

## Overview

The BC Parks API provides access to structured data about parks in
British Columbia, including:

-   Protected areas (parks, conservancies, ecological reserves, recreation areas)
-   Activities (hiking, camping, kayaking, etc.)
-   Facilities (washrooms, campgrounds, picnic shelters, etc.)
-   Public advisories (closures, wildfires, weather alerts, wildlife)
-   Park photos (metadata + downloadable image URLs)

The API is **GraphQL only**, served by a Strapi CMS backend behind the
provincial API gateway.

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

NOTE: there is **no** top-level `parks` query. The real query is
`protectedAreas` (or, with pagination, `protectedAreas_connection`).

curl -X POST https://bcparks.api.gov.bc.ca/graphql\
-H "Content-Type: application/json"\
-H "x-api-key: YOUR_API_KEY"\
-d '{ "query": "{ protectedAreas_connection(pagination: { page: 1, pageSize: 5 }) { pageInfo { total } nodes { orcs protectedAreaName latitude longitude } } }" }'

------------------------------------------------------------------------

## GraphQL Playground

https://bcparks.api.gov.bc.ca/graphql

(Tip: run `python -m src.introspect` from this repo to dump the full
schema to `data/schema.json` -- it's the most reliable reference.)

------------------------------------------------------------------------

## Real Schema (Strapi pattern)

List queries follow Strapi's `*_connection` convention. They take a
`pagination: { page, pageSize }` argument and return:

```graphql
{
  pageInfo { page pageSize pageCount total }
  nodes { ... }
}
```

### Useful list roots

| Query                          | What it returns                            |
|--------------------------------|--------------------------------------------|
| `protectedAreas_connection`    | The 1,052 parks/conservancies/etc.         |
| `publicAdvisories_connection`  | Advisories: closures, fires, wildlife      |
| `parkActivities_connection`    | (park × activity) rows                     |
| `parkFacilities_connection`    | (park × facility) rows                     |
| `parkPhotos_connection`        | Photo metadata incl. `imageUrl`            |
| `parkOperationDates_connection`| Seasonal open/close dates                  |

### Join key

Use `orcs` (Int) to join everything back to a park. It is the official
Provincial ORCS number.

### Example: paginated parks

```graphql
query Parks($page: Int!, $pageSize: Int!) {
  protectedAreas_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      orcs
      protectedAreaName
      type
      class
      latitude
      longitude
      totalArea
      establishedDate
      url
    }
  }
}
```

Note that lat/lon are **flat fields**, not nested under a `location`
object.

------------------------------------------------------------------------

## Permissions / Forbidden roots

The default API key tier blocks two useful roots:

-   `geoShapes` -- park polygon boundaries
-   `audioClips` -- Indigenous place-name recordings

Workaround for `geoShapes`: the same authoritative polygons are public
on BC's open-data portal as a Tantalis WFS layer. See
`src/download_boundaries.py` in this repo.

------------------------------------------------------------------------

## Common Issues

1.  Wrong header → must use `x-api-key` (not `Authorization` or `apikey`)
2.  Wrong query → no `parks` root; use `protectedAreas` /
    `protectedAreas_connection`
3.  Wrong field paths → lat/lon are flat, not under `location { ... }`
4.  Forgetting `pagination` → list queries default to a small page;
    paginate to get everything
5.  `Forbidden access` errors → your key tier doesn't include that
    collection (`geoShapes`, `audioClips`)

------------------------------------------------------------------------

## Recommended Architecture

1.  Backend wrapper (Python httpx in this repo; could be Go / Node)
2.  Cache layer (Redis or local files) -- the API is rate-limited
3.  Normalize API data into pandas / parquet for analysis
4.  Frontend / notebooks call your wrapper, never the API directly

------------------------------------------------------------------------

## Notes

-   Public docs are limited; the GraphQL introspection is the source
    of truth.
-   Image binaries are hosted on `nrs.objectstore.gov.bc.ca` and are
    publicly fetchable -- no API key needed for the `imageUrl` GET.
-   Build an abstraction layer (see `src/client.py`) so you can swap
    the backend if BC Parks ever moves off Strapi.
