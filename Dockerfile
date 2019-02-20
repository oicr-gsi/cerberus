FROM maven:3.6.0-jdk-8-alpine as builder
COPY . /cerberus/
RUN cd /cerberus && mvn clean package -q -B

FROM tomcat:8-jre8-alpine
COPY docker/tomcat-conf/* /usr/local/tomcat/conf/
COPY docker/webapp-conf/* /usr/local/tomcat/conf/Catalina/localhost/
RUN rm -rf /usr/local/tomcat/webapps/ROOT* \
 && echo "export CATALINA_OPTS=-Djava.security.egd=file:/dev/./urandom" > /usr/local/tomcat/bin/setenv.sh \
 && chmod a+x /usr/local/tomcat/bin/setenv.sh
COPY --from=builder /cerberus/cerberus-webapp/target/*.war /usr/local/tomcat/webapps/ROOT.war
EXPOSE 8080
CMD ["catalina.sh", "run"]
