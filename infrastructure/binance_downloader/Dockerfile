FROM fedora:latest

#RUN dnf -y update && dnf -y git-all  && dnf clean all
RUN dnf -y update
RUN git clone https://github.com/pregenRobot/bogdanoff.git

ENTRYPOINT ["tail", "-f", "/dev/null"]
