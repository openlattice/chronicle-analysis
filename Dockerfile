FROM debian:latest

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.6.14-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

RUN conda install pandas numpy
RUN pip install auth0-python pyyaml

WORKDIR /apiclients
RUN git clone https://github.com/Lattice-Works/api-clients.git
WORKDIR /apiclients/api-clients/python
RUN python setup.py install

RUN mkdir /chroniclepy
ADD chroniclepy /chroniclepy
WORKDIR /chroniclepy
RUN python setup.py install

ENTRYPOINT ["python", "-u", "/chroniclepy/run.py"]
