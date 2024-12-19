# Cerberus

Cerberus is joining system that allows merging different kinds of records. We
use it to join LIMS metadata from [Pinery](https://github.com/oicr-gsi/pinery)
with workflow metadata from [Víðarr](https://oicr-gsi.github.io/vidarr/).

To join data from one or more Vidarr servers with one or more Pinery servers,
create a configuration file ending in `.cerberus` as follows:

     {
       "pinery": {
         "pinery-miso": {
           "url": "http://pinery.example.com/",
           "versions": [
             2,
             7,
             8
           ]
         }
       },
       "vidarr": {
         "prod": "http://vidarr-prod.example.com:8000"
       },
       "ignore": [
         "example-bad-provider"
       ]
     }

The `"pinery"` section describes all Pinery instances that can be used LIMS
data sources. The keys are the provider name used in Vidarr. For each Pinery
instances, multiple versions of the same data can be used by specifying them in
the `"versions"` list.

The `"vidarr"` section describes all the Vidarr instances that should be used
as file sources. The keys are the _internal name_ of that Vidarr instance and
the value is the URL of that instance.

The `"ignore"` section contains all the LIMS provider names which are present
in the Vidarr instances' external keys but should NOT be merged when building
file provenance. If a Vidarr workflow run contains a single external key with data
from one of these ignore providers, the entire workflow run will be excluded.

To build Cerberus locally:

    mvn clean install dependency:copy-dependencies

The Cerberus file provenance client can be used to produce a joined file
provenance TSV in the traditional format using:

    java --module-path "$(find ./*/target/ ./*/target/dependency/ \
        -maxdepth 1 -mindepth 1 -iname "*.jar" | tr '\n' :)" \
        -m ca.on.oicr.gsi.cerberus.cli/ca.on.oicr.gsi.cerberus.cli.Main online  \
        -c config.json -o output.tsv.gz


