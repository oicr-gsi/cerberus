Cerberus: Provenance API Web Service
====================================

Cerberus is intended as a web service wrapper for the existing GSI Provenance API: <https://github.com/oicr-gsi/provenance>

The Cerberus project was started in May 2018 and is currently in an initial development state.


Goals of Cerberus
-----------------

- Encapsulating the file provenance code and its dependencies for ease of use
- Improved speed and efficiency, eg. by caching
- Allow provenance data to be consumed in non-Java languages


Installation
------------

See `INSTALL.md`.


Conventions
-----------

- Cerberus has a changelog: See CHANGELOG.md and https://keepachangelog.com/en/1.0.0/
- Cerberus follows Semantic Versioning: See https://semver.org/


Note on Data Size
-----------------

- Cerberus has been used to download the entire set of file provenance records (~1.8 million as of Sep-2018).
- JSON output is ~100GB uncompressed, or ~12GB compressed by gzip. Compression is recommended.
