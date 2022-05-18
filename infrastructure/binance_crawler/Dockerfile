FROM debian:latest

RUN apt update -y && apt upgrade -y

RUN apt install -y git

RUN git clone https://github.com/pregenRobot/bogdanoff.git

ENTRYPOINT ["tail", "-f", "/dev/null"]
