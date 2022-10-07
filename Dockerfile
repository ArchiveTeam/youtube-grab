FROM atdr.meo.ws/archiveteam/grab-base
RUN apt-get update \
  && apt-get install -y --no-install-recommends nodejs \
  && rm -rf /var/lib/apt/lists/*
