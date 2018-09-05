Cerberus: Changelog
===================

Unreleased
----------

- Caching for provenance results
- Integration tests using "real" databases and web server

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
