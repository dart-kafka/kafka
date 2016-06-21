FROM ches/kafka:0.10.0.0

USER root

ADD config /kafka/config
ADD start.sh /start.sh

RUN chown -R kafka:kafka /kafka /data /logs
RUN chown -R kafka:kafka /start.sh
RUN chmod 755 /start.sh

USER kafka
ENV PATH /kafka/bin:$PATH
WORKDIR /kafka

EXPOSE 9092
VOLUME [ "/data", "/logs" ]

CMD ["/start.sh"]
