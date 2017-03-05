FROM ches/kafka:0.10.1.0

USER root

ADD config /kafka/config
ADD start.sh /start.sh

RUN mkdir /data1 /data2

RUN chown -R kafka:kafka /kafka /data1 /data2 /logs
RUN chown -R kafka:kafka /start.sh
RUN chmod 755 /start.sh

USER kafka
ENV PATH /kafka/bin:$PATH
WORKDIR /kafka

EXPOSE 9092 9093
VOLUME [ "/data1", "/data2", "/logs" ]

CMD ["/start.sh"]
