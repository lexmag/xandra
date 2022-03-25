ARG CASSANDRA_VERSION=4

FROM cassandra:$CASSANDRA_VERSION

ARG AUTHENTICATION=false

# Enable user-defined functions.
RUN sed -i -e "s/\(enable_user_defined_functions: \)false/\1true/" /etc/cassandra/cassandra.yaml

RUN if [ "$AUTHENTICATION" = true ]; then \
  sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/cassandra/cassandra.yaml; \
  fi
