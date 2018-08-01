Cerberus: Changelog
===================

Unreleased
----------

- Provenance for Lane and Sample, as well as File
- Caching for provenance results
- Integration tests using "real" databases and web server

Version 0.1.0: 2018-08-01
-------------------------

- Initial prototype of Cerberus, a provenance API web service
- ProvenanceServlet receives JSON input from the Client class; returns JSON file provenance
- RequestParser parses input JSON into arguments for the ProvenanceHandler
- ProvenanceHandler sets up Provenance providers and downloads file provenance results
- WebProvenanceServlet is a placeholder web interface, accessed by index.html
- Unit tests with Mockito
