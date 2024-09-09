FROM redhat/ubi9-minimal:9.4-1227
RUN microdnf install -y --nodocs python3 python3-pip procps-ng findutils && \
    microdnf clean all && \
    pip-3 install 'PyYAML>=5.4.1' && \
    pip-3 install 'pytz==2023.3.post1' && \
    pip-3 install 'tg-archive==1.1.3' && \
    pip-3 install 'colorama' && \
    pip-3 install 'humanize' && \
    pip-3 install 'telethon'
CMD ["sh", "-c", "sleep infinity"]
WORKDIR /app
ENV HOME=/app
ARG UID
ARG GID
ENV TZ=${TZ} UID=${UID} GID=${GID}
COPY . /app
RUN chmod +x /app/index.sh
RUN groupadd -g ${GID:-1000} app && \
    useradd -u ${UID:-1000} -g app app && \
    mkdir -p /app /data && \
    chown -R app:app /app /data
VOLUME ["/session"]
USER app
CMD ["/app/index.sh"]
