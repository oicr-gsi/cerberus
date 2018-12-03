Cerberus: Changelog
===================

Unreleased
----------

- Caching for provenance results
- Integration tests using "real" databases and web server
- Improved documentation and argument checking for CLI
- GP-1862: Remove /provenance suffix from URL
- GP-1876: Split Cerberus into modules
- GP-1877: Move Provenance configuration from client to server


Version 0.3.1: 2018-09-27
-------------------------

Added:
- Simple command-line interface: Cerberus.java
- Error handling in the HTTP servlet; errors reported in HTTP response


Version 0.3.0: 2018-09-18
-------------------------

Added:
- Streaming I/O for robust handling of large datasets
- JSON parsing on the fly instead of reading the entire tree at once

Changed:
- Base class deleted; POST field names moved to the PostField enumeration
- Moved utility classes into a 'util' subpackage


Version 0.2.0: 2018-09-05
-------------------------

Added:
- Provenance for Analysis, File, Lane, Sample
- CerberusClient implements the ExtendedProvenanceClient interface, along with other methods from the DefaultProvenanceClient implementation
- Classes to deserialize JSON into provenance objects
- Enumeration for provenance types: Analysis, File, Lane, Sample
- Enumeration for provenance actions: No filter, inclusion filter, inclusion & exclusion filters, by provider, by provider & ID. The actions correspond to methods in ExtendedProvenanceClient.
- Dummy provenance data in JSON format for tests

Changed:
- Client renamed as ProvenanceHttpClilent
- ProvenanceServlet uses RequestParser to parse 4 fields from JSON: Type, action, inclusion filters, exclusion filters.
- Type and action must be members of the respective enumerations; filter arguments may be empty.


Version 0.1.0: 2018-08-01
-------------------------

- Initial prototype of Cerberus, a provenance API web service
- ProvenanceServlet receives JSON input from the Client class; returns JSON file provenance
- RequestParser parses input JSON into arguments for the ProvenanceHandler
- ProvenanceHandler sets up Provenance providers and downloads file provenance results
- WebProvenanceServlet is a placeholder web interface, accessed by index.html
- Unit tests with Mockito
