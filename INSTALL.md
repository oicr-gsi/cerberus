Cerberus Installation
=====================

The following instructions assume Cerberus is being installed to an Apache Tomcat web server.


Cerberus .war file
------------------

- File name is `cerberus-webapp-${VERSION}.war`, eg. `cerberus-webapp-0.4.0.war`
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

Create a file called `$NAME.xml` in `$CATALINA_HOME/conf/Catalina/localhost`, creating the directory if necessary. `$NAME` is the name of the Cerberus webapp deployment, ie. `cerberus-webapp-${VERSION}`. For example, if `$CATALINA_HOME` is `/opt/tomcat`, and release version is 0.4.0, the full path is `/opt/tomcat/conf/Catalina/localhost/cerberus-webapp-0.4.0.xml`.

Populate the XML file with the following information:

```
<Context>
   <Parameter name="provenanceProviderSettings" value="/my/provider/settings/file.json" override="false"/>
</Context>
```

The directory and filename for the provider settings JSON file may be any values of the user's choice. See the [Apache Tomcat documentation](https://tomcat.apache.org/tomcat-8.0-doc/config/context.html) for further details on context parameters.
