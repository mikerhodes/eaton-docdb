# eaton-docdb

This is a demonstration of a JSON database index. It indexes all fields in each
JSON document that it's given and allows simple queries for documents.

## Background

This repository was originally forked from
[eatonphil/docdb: Basic document db from scratch in Go](https://github.com/eatonphil/docdb),
which was a repository containing the code for the article
[Writing a document database from scratch in Go: Lucene-like filters and indexes | notes.eatonphil.com](https://notes.eatonphil.com/documentdb.html).

I've rewritten most of the code since I forked it but the basic structure that
Phil put in place was super useful. What I have aimed to do is to make a more
complete indexing library, to teach myself some of the concepts beyond Phil's
article.

There is a short series of articles describing this work at
[the byodi (build your own database index) tag on dx13.co.uk](https://dx13.co.uk/tags/byodi/).

I added:

- Support for indexing native JSON data types (null, string, numbers, booleans)
  whereas the original code converted everything to strings.
- Ability to update/delete documents from the index. The original code only
  allowed adding documents.

I also fixed a few bugs, although sadly too late after I'd started my changes to
PR them.

## Build

Grab Go 1.18 and this repo. Inside this repo run:

```bash
$ go build
$ ./docdb
```

## Usage

_I have mostly tested my changes using unit tests. I'm not sure if these curl
calls still work._

Then in another terminal:

```bash
$ curl -X POST -H 'Content-Type: application/json' -d '{"name": "Kevin", "age": "45"}' http://localhost:8080/docs
{"body":{"id":"5ac64e74-58f9-4ba4-909e-1d5bf4ddcaa1"},"status":"ok"}
$ curl --get http://localhost:8080/docs --data-urlencode 'q=name:"Kevin"' | jq
{
  "body": {
    "count": 1,
    "documents": [
      {
        "body": {
          "age": "45",
          "name": "Kevin"
        },
        "id": "5ac64e74-58f9-4ba4-909e-1d5bf4ddcaa1"
      }
    ]
  },
  "status": "ok"
}
$ curl --get http://localhost:8080/docs --data-urlencode 'q=age:<50' | jq
{
  "body": {
    "count": 1,
    "documents": [
      {
        "body": {
          "age": "45",
          "name": "Kevin"
        },
        "id": "5ac64e74-58f9-4ba4-909e-1d5bf4ddcaa1"
      }
    ]
  },
  "status": "ok"
}
```
