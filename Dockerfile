FROM quay.io/astronomer/astro-runtime:4.2.0

USER root
RUN apt-get -qq update && apt-get -qq install zip unzip

USER astro
