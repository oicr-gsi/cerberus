Cerberus Installation
=====================

The following instructions assume Cerberus is being installed to an Apache Tomcat web server.


Cerberus .war file
------------------

- Copy the `.war` file to `$TOMCAT_HOME/webapps`.
- If replacing a previous installation, delete the uncompressed folder from `webapps`
- Restart Tomcat so changes take effect: `sudo systemctl restart tomcat`


Memory configuration
--------------------

Configure an appropriate memory limit on the server, by placing the following line in `$TOMCAT_HOME/bin/setenv.sh`:

```
CATALINA_OPTS="$CATALINA_OPTS -Xms4G -Xmx8G -server"
```


Provenance provider configuration
---------------------------------

Cerberus requires a provenance provider settings JSON file. It is in the format defined by the `ProviderLoader` class in the `oicr-gsi/pipedev` repository. Because this file contains database credentials, it is kept on the server instead of being supplied by the client.

Location of the provider JSON file is specified as follows:

Create a file called `$NAME.xml` in `$CATALINA_HOME/conf/Catalina/localhost`, creating the directory if necessary. `$NAME` is the name of the Cerberus deployment, eg. `cerberus-1.0-SNAPSHOT`. For example, if `$CATALINA_HOME` is `/opt/tomcat`, the full path is `/opt/tomcat/conf/Catalina/localhost/cerberus-1.0-SNAPSHOT.xml`.

Populate the XML file with the following information:

```
<Context>
   <Parameter name="provenanceProviderSettings" value="/directory/for/settings/file/providerSettings.json" override="false"/>
</Context>
```

The directory and filename for the provider settings JSON file may be any values of the user's choice. See the [Apache Tomcat documentation](https://tomcat.apache.org/tomcat-7.0-doc/config/context.html) for further details on context parameters.
