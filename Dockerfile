FROM r-base:latest
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts

RUN apt-get update \ 
 && apt-get install -y sudo

RUN apt-get install -y build-essential libssl-dev libxml2-dev libcurl4-openssl-dev

RUN apt-get install r-base sudo
RUN apt-get install r-base-dev sudo
RUN R -e "install.packages(c('curl', 'httr', 'xml2', 'aws.s3', 'fst', 'aws.ec2metadata'), repos='http://cran.r-project.org', INSTALL_opts='--no-html')"


CMD ["Rscript", "compressScript.R"]
