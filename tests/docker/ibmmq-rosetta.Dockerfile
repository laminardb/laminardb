# See ibmmq-rosetta.compose.yml: renames argv[0] so the single-instance check passes under Rosetta.
FROM icr.io/ibm-messaging/mq:9.4.0.26-r2
USER 0
RUN mv /usr/local/bin/runmqserver /usr/local/bin/runmqserver.real \
 && printf '#!/bin/bash\nexec -a mqserver /usr/local/bin/runmqserver.real "$@"\n' > /usr/local/bin/runmqserver \
 && chmod 755 /usr/local/bin/runmqserver
USER 1001
ENTRYPOINT ["bash", "-c", "exec -a mqdevserver /usr/local/bin/runmqdevserver"]
