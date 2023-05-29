# Datapipe

`datapipe` is a realtime incremental ETL library for Python application.

# Development

At the moment these branches are active:

* `master` - current development state, will be promoted to `0.13.x` series
  release once ready
* `v0.12` - current stable version
* `v0.11` - legacy stable version

# Version compatibility

At the moment, the datapipe library is under active development. Versions:
`v0.*.*`

It should be expected that each minor version is not backward compatible with
the previous one. That is, `v0.7.0` is not compatible with `v0.6.1`. Dependencies
should be fixed to the exact minor version.

After stabilization and transition to the major version `v1.*.*`, the common
rules will apply: all versions with the same major component are compatible.
